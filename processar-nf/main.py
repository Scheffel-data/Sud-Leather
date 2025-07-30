import os
from datetime import datetime
import uuid
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
        # CÓDIGO CORRIGIDO
        if data_emissao_str:
            try:
                date_obj = None 
                if 'T' in data_emissao_str:
                    date_str_sem_fuso = data_emissao_str.split('T')[0]
                    date_obj = datetime.strptime(date_str_sem_fuso, '%Y-%m-%d').date()
                else:
                    date_obj = datetime.strptime(data_emissao_str, '%Y-%m-%d').date()

                if date_obj:
                    data_emissao_formatada = date_obj.isoformat() # Converte para string "AAAA-MM-DD"
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

import json


@app.route("/", methods=["POST"])
def process_nfe_xml():
    try:
        data = request.get_json(silent=True)
        print("📥 Payload recebido:", json.dumps(data, indent=2))

        if not data or "message" not in data:
            print("❌ Erro: JSON malformado ou sem campo 'message'.")
            return "Requisição inválida", 400

        message = data["message"]
        attributes = message.get("attributes", {})
        bucket_name = attributes.get("bucketId")
        file_name = attributes.get("objectId")

        if not bucket_name or not file_name:
            print("⚠️ Payload incompleto. bucketId ou objectId ausente.")
            return "Campos obrigatórios ausentes", 400

        if not file_name.lower().endswith(".xml") or not file_name.startswith("recebidas/"):
            print(f"📁 Arquivo ignorado: {file_name}")
            return f"Ignorado: {file_name}", 200

        print(f"📂 Processando arquivo: {file_name} do bucket: {bucket_name}")

        # Baixa conteúdo XML
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        xml_content = blob.download_as_text()

        # Cria o DataFrame
        df_nfe = criar_df_nfe(xml_content)

        if df_nfe is None or df_nfe.empty:
            # Lógica para mover para uma pasta de erro de parsing, se desejado
            print(f"⚠️ Nenhum dado extraído de {file_name}")
            return "Sem dados válidos", 400

        # --- NOVA LÓGICA DE MERGE ---
        temp_table_id = f"temp_nfe_{uuid.uuid4().hex}"
        temp_table_ref = bigquery_client.dataset(DATASET_ID).table(temp_table_id)

        try:
            # 1. Enviar DataFrame para uma tabela temporária no BigQuery
            job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_TRUNCATE")
            bigquery_client.load_table_from_dataframe(df_nfe, temp_table_ref, job_config=job_config).result()
            print(f"Dados carregados na tabela temporária: {temp_table_id}")

            # 2. Construir e executar a query MERGE
            merge_query = f"""
                MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS T
                USING `{PROJECT_ID}.{DATASET_ID}.{temp_table_id}` AS S
                ON T.numero_nf = S.numero_nf AND T.Descricao = S.Descricao
                WHEN NOT MATCHED THEN
                  INSERT (numero_nf, data_emissao, emitente, CNPJ, Descricao, Quantidade_pcs, Quantidade_kg, valor_unitario, valor_total_produto)
                  VALUES(S.numero_nf, S.data_emissao, S.emitente, S.CNPJ, S.Descricao, S.Quantidade_pcs, S.Quantidade_kg, S.valor_unitario, S.valor_total_produto)
            """
            merge_job = bigquery_client.query(merge_query)
            merge_job.result()  # Espera a query terminar

            if merge_job.errors:
                print(f"❌ Erros ao executar MERGE: {merge_job.errors}")
                # Aqui você pode mover o arquivo para uma pasta de erro de MERGE
                return "Erro ao executar MERGE", 500
            else:
                print(f"✅ MERGE concluído com sucesso para o arquivo {file_name}.")
                # Move o arquivo para "processados" após o sucesso
                now = datetime.now()
                destination_folder = f"processados/{now.year:04d}/{now.month:02d}"
                new_path = f"{destination_folder}/{file_name.split('/')[-1]}"
                bucket.copy_blob(blob, bucket, new_path)
                blob.delete()
                print(f"✅ Processado e movido para: {new_path}")
                return f"Processado: {file_name}", 200

        finally:
            # 3. Apagar a tabela temporária, aconteça o que acontecer
            bigquery_client.delete_table(temp_table_ref, not_found_ok=True)
            print(f"Tabela temporária {temp_table_id} apagada.")

    except Exception as e:
        print(f"🔥 Erro crítico: {str(e)}")
        return f"Erro interno: {str(e)}", 500

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))