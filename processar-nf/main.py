import os
from datetime import datetime
import uuid
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, request
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions

# --- Configurações do BigQuery ---
PROJECT_ID = "sud-leather"
DATASET_ID = "RAW_DATA"
TABLE_ID = "Frigorifico_Nota_Fiscal"

# Inicializa os clientes do Cloud Storage e BigQuery
storage_client = storage.Client()
bigquery_client = bigquery.Client(project=PROJECT_ID)

app = Flask(__name__)

def criar_df_nfe(xml_content):
    """
    Processa o conteúdo de um XML de NFe e retorna um DataFrame do Pandas com os dados dos produtos.
    """
    try:
        namespaces = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//nfe:infNFe', namespaces)

        if infNFe is None:
            print("Tag <infNFe> não encontrada no XML.")
            return None

        # --- Extração de dados do cabeçalho da NFe ---
        numero_nf = infNFe.find('.//nfe:ide/nfe:nNF', namespaces).text
        data_emissao_str = (infNFe.find('.//nfe:ide/nfe:dhEmi', namespaces) or infNFe.find('.//nfe:ide/nfe:dEmi', namespaces)).text
        emitente_nome = infNFe.find('.//nfe:emit/nfe:xNome', namespaces).text
        emitente_cnpj = infNFe.find('.//nfe:emit/nfe:CNPJ', namespaces).text
        qvol_element = infNFe.find('.//nfe:transp/nfe:vol/nfe:qVol', namespaces)
        quantidade_pecas = int(float(qvol_element.text)) if qvol_element is not None and qvol_element.text else 0
        
        # Formata a data de emissão
        date_str_sem_fuso = data_emissao_str.split('T')[0]
        data_emissao_formatada = datetime.strptime(date_str_sem_fuso, '%Y-%m-%d').date()

        # --- Extração dos itens da NFe ---
        lista_produtos = []
        for det in infNFe.findall('.//nfe:det', namespaces):
            produto = {
                'numero_nf': int(numero_nf),
                'data_emissao': data_emissao_formatada,
                'emitente': emitente_nome,
                'CNPJ': emitente_cnpj,
                'Descricao': det.find('.//nfe:prod/nfe:xProd', namespaces).text,
                'Quantidade_pcs': quantidade_pecas,
                'Quantidade_kg': float(det.find('.//nfe:prod/nfe:qCom', namespaces).text or '0'),
                'valor_unitario': float(det.find('.//nfe:prod/nfe:vUnCom', namespaces).text or '0'),
                'valor_total_produto': float(det.find('.//nfe:prod/nfe:vProd', namespaces).text or '0')
            }
            lista_produtos.append(produto)

        return pd.DataFrame(lista_produtos)

    except Exception as e:
        print(f"❌ Erro ao processar o conteúdo do XML: {e}")
        return None

def mover_blob_para(bucket, blob, pasta_destino):
    """
    Move um blob para uma pasta de destino (erros ou processados).
    """
    try:
        now = datetime.now()
        destination_folder = f"{pasta_destino}/{now.year:04d}/{now.month:02d}"
        new_path = f"{destination_folder}/{os.path.basename(blob.name)}"
        
        bucket.copy_blob(blob, bucket, new_path)
        blob.delete()
        print(f"✅ Arquivo movido para: {new_path}")
    except Exception as e:
        print(f"🔥 Falha crítica ao mover o arquivo {blob.name} para {pasta_destino}. Erro: {e}")

@app.route("/", methods=["POST"])
def process_nfe_xml():
    """
    Função principal, agora adaptada para o formato de payload do Eventarc (CloudEvents).
    """
    # O Eventarc envia um payload JSON que representa um CloudEvent.
    event = request.get_json(silent=True)
    if not event:
        print("Requisição inválida, sem payload JSON.")
        return "Requisição inválida", 400

    # Os dados específicos do evento (como nome do bucket e arquivo) estão no campo 'data'.
    data = event.get('data', {})
    bucket_name = data.get('bucket')
    file_name = data.get('name') # No CloudEvents, o caminho do arquivo está em 'name'.

        # --- Filtro de pasta implementado diretamente no código ---
    if not file_name or 'recebidas/' not in file_name:
        print(f"📁 Arquivo ignorado (fora da pasta 'recebidas/'): {file_name}")
        return "Arquivo ignorado", 200
    
    # Validação para garantir que temos as informações necessárias do evento
    if not bucket_name:
        print(f"❌ Erro no payload do evento: 'bucket' não encontrado.")
        return "Payload do evento inválido", 400

    print(f"📂 Processando arquivo: {file_name} do bucket: {bucket_name}")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        xml_content = blob.download_as_text()
    except exceptions.NotFound:
        print(f"❌ Erro 404: Arquivo {file_name} não encontrado no bucket. Mensagem descartada.")
        return "Arquivo não encontrado, mensagem descartada", 200

    df_nfe = criar_df_nfe(xml_content)

    if df_nfe is None or df_nfe.empty:
        print(f"⚠️ Nenhum dado extraído de {file_name}. Movendo para a pasta de erros.")
        mover_blob_para(bucket, blob, "erros")
        return "Arquivo com dados inválidos ou vazios", 200

    temp_table_id = f"temp_nfe_{uuid.uuid4().hex}"
    temp_table_ref = bigquery_client.dataset(DATASET_ID).table(temp_table_id)

    try:
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_TRUNCATE")
        bigquery_client.load_table_from_dataframe(df_nfe, temp_table_ref, job_config=job_config).result()
        print(f"Dados carregados na tabela temporária: {temp_table_id}")

        merge_query = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS T
            USING `{PROJECT_ID}.{DATASET_ID}.{temp_table_id}` AS S
            ON T.CNPJ = S.CNPJ AND T.numero_nf = S.numero_nf AND T.Descricao = S.Descricao
            WHEN NOT MATCHED THEN
              INSERT (numero_nf, data_emissao, emitente, CNPJ, Descricao, Quantidade_pcs, Quantidade_kg, valor_unitario, valor_total_produto)
              VALUES(S.numero_nf, S.data_emissao, S.emitente, S.CNPJ, S.Descricao, S.Quantidade_pcs, S.Quantidade_kg, S.valor_unitario, S.valor_total_produto);
        """
        
        merge_job = bigquery_client.query(merge_query)
        merge_job.result()

        if merge_job.errors:
            print(f"❌ Erros ao executar MERGE: {merge_job.errors}")
            mover_blob_para(bucket, blob, "erros")
        else:
            print(f"✅ MERGE concluído com sucesso para o arquivo {file_name}.")
            mover_blob_para(bucket, blob, "processados")
        
        return "Processamento finalizado", 200

    except Exception as e:
        print(f"🔥 Erro crítico durante o processo de BigQuery: {str(e)}")
        mover_blob_para(bucket, blob, "erros")
        return f"Erro interno no BigQuery: {str(e)}", 200

    finally:
        bigquery_client.delete_table(temp_table_ref, not_found_ok=True)
        print(f"Tabela temporária {temp_table_id} apagada.")

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
