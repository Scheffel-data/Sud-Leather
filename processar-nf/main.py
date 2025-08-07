import os
from datetime import datetime
import uuid
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, request
from google.cloud import storage
from google.cloud import bigquery
import json

# --- Configurações do BigQuery ---
PROJECT_ID = "sud-leather"
# ATUALIZAÇÃO: Nome do dataset alterado para seguir as boas práticas.
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
            # MELHORIA: Extrai o número do item ('nItem'), que é crucial para uma chave única.
            nItem = det.get('nItem')
            
            produto = {
                'numero_nf': int(numero_nf),
                'data_emissao': data_emissao_formatada,
                'emitente': emitente_nome,
                'CNPJ': emitente_cnpj,
                'nItem': int(nItem), # Adiciona o número do item
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

@app.route("/", methods=["POST"])
def process_nfe_xml():
    """
    Função principal da Cloud Function, acionada por um evento do Cloud Storage.
    """
    try:
        data = request.get_json(silent=True)
        if not data or "message" not in data:
            return "Requisição inválida", 400

        message = data["message"]
        attributes = message.get("attributes", {})
        bucket_name = attributes.get("bucketId")
        file_name = attributes.get("objectId")

        if not bucket_name or not file_name or not file_name.lower().endswith(".xml"):
            print(f"📁 Arquivo ignorado (não é XML ou payload inválido): {file_name}")
            return "Arquivo ignorado", 200

        print(f"📂 Processando arquivo: {file_name} do bucket: {bucket_name}")

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        xml_content = blob.download_as_text()

        df_nfe = criar_df_nfe(xml_content)

        if df_nfe is None or df_nfe.empty:
            print(f"⚠️ Nenhum dado extraído de {file_name}")
            # Você pode adicionar uma lógica para mover para uma pasta de erro aqui
            return "Sem dados válidos", 400

        # --- LÓGICA DE MERGE APRIMORADA ---
        temp_table_id = f"temp_nfe_{uuid.uuid4().hex}"
        temp_table_ref = bigquery_client.dataset(DATASET_ID).table(temp_table_id)

        try:
            # 1. Enviar DataFrame para uma tabela temporária no BigQuery
            job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_TRUNCATE")
            bigquery_client.load_table_from_dataframe(df_nfe, temp_table_ref, job_config=job_config).result()
            print(f"Dados carregados na tabela temporária: {temp_table_id}")

            # 2. Construir e executar a query MERGE com a chave correta
            # CORREÇÃO CRÍTICA: A chave do MERGE agora usa (CNPJ, numero_nf, nItem) para garantir unicidade.
            merge_query = f"""
                MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS T
                USING `{PROJECT_ID}.{DATASET_ID}.{temp_table_id}` AS S
                ON T.CNPJ = S.CNPJ AND T.numero_nf = S.numero_nf AND T.nItem = S.nItem
                WHEN NOT MATCHED THEN
                  INSERT (numero_nf, data_emissao, emitente, CNPJ, nItem, Descricao, Quantidade_pcs, Quantidade_kg, valor_unitario, valor_total_produto)
                  VALUES(S.numero_nf, S.data_emissao, S.emitente, S.CNPJ, S.nItem, S.Descricao, S.Quantidade_pcs, S.Quantidade_kg, S.valor_unitario, S.valor_total_produto);
            """
            
            print("Executando query MERGE...")
            merge_job = bigquery_client.query(merge_query)
            merge_job.result()

            if merge_job.errors:
                print(f"❌ Erros ao executar MERGE: {merge_job.errors}")
                return "Erro ao executar MERGE", 500
            else:
                print(f"✅ MERGE concluído com sucesso para o arquivo {file_name}.")
                # Mover o arquivo para "processados" após o sucesso
                now = datetime.now()
                destination_folder = f"processados/{now.year:04d}/{now.month:02d}"
                new_path = f"{destination_folder}/{os.path.basename(file_name)}"
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

if __name__ == "__main__":
    # Esta parte é para testes locais e não é usada na Cloud Function
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

