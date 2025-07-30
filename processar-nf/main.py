import os
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, request
from google.cloud import storage
from google.cloud import bigquery

# --- Configurações do BigQuery ---
PROJECT_ID = "sud-leather"
DATASET_ID = "Data_base"
TABLE_ID = "Frigorifico_Nota_Fiscal"

# Inicializa os clientes do Cloud Storage e BigQuery
storage_client = storage.Client()
bigquery_client = bigquery.Client(project=PROJECT_ID)

app = Flask(__name__)

def criar_df_nfe(xml_content):
    try:
        namespaces = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//nfe:infNFe', namespaces)

        numero_nf_element = infNFe.find('.//nfe:ide/nfe:nNF', namespaces)
        numero_nf = numero_nf_element.text if numero_nf_element is not None else None

        data_emissao_element = infNFe.find('.//nfe:ide/nfe:dhEmi', namespaces)
        if data_emissao_element is None:
            data_emissao_element = infNFe.find('.//nfe:ide/nfe:dEmi', namespaces)
        data_emissao_str = data_emissao_element.text if data_emissao_element is not None else None

        data_emissao_formatada = None
        if data_emissao_str:
            try:
                if 'T' in data_emissao_str and ('+' in data_emissao_str or '-' == data_emissao_str[-6]):
                    data_emissao_formatada = datetime.fromisoformat(data_emissao_str).date()
                elif 'T' in data_emissao_str:
                    data_emissao_formatada = datetime.strptime(data_emissao_str.split('T')[0], '%Y-%m-%d').date()
                else:
                    data_emissao_formatada = datetime.strptime(data_emissao_str, '%Y-%m-%d').date()
            except ValueError:
                data_emissao_formatada = data_emissao_str

        emitente_nome = infNFe.find('.//nfe:emit/nfe:xNome', namespaces).text if infNFe.find('.//nfe:emit/nfe:xNome', namespaces) is not None else None
        emitente_cnpj = infNFe.find('.//nfe:emit/nfe:CNPJ', namespaces).text if infNFe.find('.//nfe:emit/nfe:CNPJ', namespaces) is not None else None

        qvol_element = infNFe.find('.//nfe:transp/nfe:vol/nfe:qVol', namespaces)
        quantidade_pecas = int(float(qvol_element.text)) if qvol_element is not None and qvol_element.text else 0

        lista_produtos = []
        for det in infNFe.findall('.//nfe:det', namespaces):
            descricao = det.find('.//nfe:prod/nfe:xProd', namespaces).text if det.find('.//nfe:prod/nfe:xProd', namespaces) is not None else None
            qCom = det.find('.//nfe:prod/nfe:qCom', namespaces).text if det.find('.//nfe:prod/nfe:qCom', namespaces) is not None else '0'
            vUnCom = det.find('.//nfe:prod/nfe:vUnCom', namespaces).text if det.find('.//nfe:prod/nfe:vUnCom', namespaces) is not None else '0'
            vProd = det.find('.//nfe:prod/nfe:vProd', namespaces).text if det.find('.//nfe:prod/nfe:vProd', namespaces) is not None else '0'

            produto = {
                'numero_nf': numero_nf,
                'data_emissao': data_emissao_formatada,
                'emitente': emitente_nome,
                'CNPJ': emitente_cnpj,
                'Descricao': descricao,
                'Quantidade_pcs': quantidade_pecas,
                'Quantidade_kg': float(qCom),
                'valor_unitario': float(vUnCom),
                'valor_total_produto': float(vProd)
            }
            lista_produtos.append(produto)

        df = pd.DataFrame(lista_produtos)
        return df

    except Exception as e:
        print(f"Erro ao processar XML: {e}")
        return None

@app.route("/", methods=["POST"])
def process_nfe_xml():
    data = request.get_json()

    # Novo: Extrai os atributos de Pub/Sub
    message = data.get("message", {})
    attributes = message.get("attributes", {})
    
    bucket_name = attributes.get("bucketId")
    file_name = attributes.get("objectId")

    if not bucket_name or not file_name:
        return "Erro: bucketId ou objectId não encontrado no payload", 400

    if not file_name.lower().endswith('.xml') or not file_name.startswith('recebidas/'):
        return f"Ignorado: {file_name} não é XML ou não está na pasta 'recebidas'", 200


    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        xml_content = blob.download_as_text()
        df_nfe = criar_df_nfe(xml_content)

        if df_nfe is not None and not df_nfe.empty:
            rows_to_insert = df_nfe.to_dict(orient='records')
            table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
            errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

            if errors == []:
                destination_folder = f"processados/{datetime.now().year:04d}/{datetime.now().month:02d}"
                base_file_name = file_name.split('/')[-1]
                new_file_path = f"{destination_folder}/{base_file_name}"
                new_blob = bucket.blob(new_file_path)
                blob.rewrite(new_blob)
                blob.delete()
                return f"Processado e movido para {new_file_path}", 200
            else:
                return f"Erros ao inserir no BigQuery: {errors}", 500
        else:
            return f"Nenhum dado extraído de {file_name}", 400

    except Exception as e:
        return f"Erro crítico ao processar {file_name}: {e}", 500

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
