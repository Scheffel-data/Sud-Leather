import os
from datetime import datetime
import uuid
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, request
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions
import json

# --- Configurações do BigQuery ---
PROJECT_ID = "sud-leather"
DATASET_ID = "RAW_DATA"
TABLE_ID = "Frigorifico_Nota_Fiscal"

# Inicializa os clientes do Cloud Storage e BigQuery
storage_client = storage.Client()
bigquery_client = bigquery.Client(project=PROJECT_ID)

app = Flask(__name__)

def get_element_text(element, path, namespaces, default=None):
    """
    Função auxiliar para encontrar um sub-elemento e retornar o seu texto.
    Retorna um valor padrão se o elemento não for encontrado.
    """
    if element is None:
        return default
    found_element = element.find(path, namespaces)
    if found_element is not None and found_element.text is not None:
        return found_element.text.strip()
    return default

def criar_df_nfe(xml_content):
    """
    Processa o conteúdo de um XML de NFe de forma robusta e retorna um DataFrame.
    """
    try:
        namespaces = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//nfe:infNFe', namespaces)

        if infNFe is None:
            print("❌ Tag <infNFe> não encontrada no XML.")
            return None

        # --- Extração de dados do cabeçalho da NFe (de forma segura) ---
        ide_element = infNFe.find('nfe:ide', namespaces)
        emit_element = infNFe.find('nfe:emit', namespaces)
        transp_element = infNFe.find('nfe:transp', namespaces)

        if ide_element is None:
            print("❌ Tag essencial <ide> não encontrada.")
            return None
        if emit_element is None:
            print("❌ Tag essencial <emit> não encontrada.")
            return None

        # Validação campo a campo para logs mais claros
        numero_nf = get_element_text(ide_element, 'nfe:nNF', namespaces)
        if not numero_nf:
            print("❌ Campo obrigatório não encontrado no XML: nNF (Número da Nota Fiscal)")
            return None

        data_emissao_str = get_element_text(ide_element, 'nfe:dhEmi', namespaces) or get_element_text(ide_element, 'nfe:dEmi', namespaces)
        if not data_emissao_str:
            print("❌ Campo obrigatório não encontrado no XML: dhEmi ou dEmi (Data de Emissão)")
            return None
            
        emitente_nome = get_element_text(emit_element, 'nfe:xNome', namespaces)
        if not emitente_nome:
            print("❌ Campo obrigatório não encontrado no XML: xNome (Nome do Emitente)")
            return None

        emitente_cnpj = get_element_text(emit_element, 'nfe:CNPJ', namespaces)
        if not emitente_cnpj:
            print("❌ Campo obrigatório não encontrado no XML: CNPJ (CNPJ do Emitente)")
            return None

        qvol_text = get_element_text(transp_element, 'nfe:vol/nfe:qVol', namespaces, '0')
        quantidade_pecas = int(float(qvol_text))

        # Formata a data de emissão
        date_str_sem_fuso = data_emissao_str.split('T')[0]
        data_emissao_formatada = datetime.strptime(date_str_sem_fuso, '%Y-%m-%d').date()

        # --- Extração dos itens da NFe ---
        lista_produtos = []
        for det in infNFe.findall('.//nfe:det', namespaces):
            prod_element = det.find('nfe:prod', namespaces)
            if prod_element is None:
                continue # Pula para o próximo item se a tag <prod> não existir

            produto = {
                # --- CORREÇÃO: numero_nf como STRING ---
                'numero_nf': numero_nf,
                'data_emissao': data_emissao_formatada,
                'emitente': emitente_nome,
                # --- CORREÇÃO: CNPJ como STRING (o tipo numérico foi removido mais abaixo) ---
                'CNPJ': emitente_cnpj,
                'Descricao': get_element_text(prod_element, 'nfe:xProd', namespaces, ''),
                'Quantidade_pcs': quantidade_pecas,
                'Quantidade_kg': float(get_element_text(prod_element, 'nfe:qCom', namespaces, '0')),
                'valor_unitario': float(get_element_text(prod_element, 'nfe:vUnCom', namespaces, '0')),
                'valor_total_produto': float(get_element_text(prod_element, 'nfe:vProd', namespaces, '0'))
            }
            lista_produtos.append(produto)
        
        if not lista_produtos:
            print("⚠️ Nenhum item (<det>) encontrado na NFe.")
            return None

        return pd.DataFrame(lista_produtos)

    except ET.ParseError as e:
        print(f"❌ Erro de parsing no XML. O ficheiro pode estar malformado: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro inesperado ao processar o conteúdo do XML: {e}")
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
    event = request.get_json(silent=True)
    if not event:
        print("❌ Requisição inválida, sem payload JSON.")
        return "Requisição inválida", 400

    print(f"📦 Evento recebido: {json.dumps(event)}")

    bucket_name = event.get('bucket')
    file_name = event.get('name')

    if not file_name or 'recebidas/' not in file_name:
        print(f"📁 Arquivo ignorado (não está na pasta 'recebidas/'): {file_name}")
        return "Arquivo ignorado", 200
    
    if not bucket_name:
        print(f"❌ Erro no payload do evento: 'bucket' não encontrado.")
        return "Payload do evento inválido", 400

    print(f"🚀 Processando arquivo: {file_name} do bucket: {bucket_name}")
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

    # --- CORREÇÃO: Bloco de conversão do CNPJ para número foi REMOVIDO para corresponder ao esquema STRING ---

    temp_table_id = f"temp_nfe_{uuid.uuid4().hex}"
    temp_table_ref = bigquery_client.dataset(DATASET_ID).table(temp_table_id)

    try:
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_TRUNCATE")
        bigquery_client.load_table_from_dataframe(df_nfe, temp_table_ref, job_config=job_config).result()
        print(f"💾 Dados carregados na tabela temporária: {temp_table_id}")

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
        print(f"🗑️ Tabela temporária {temp_table_id} apagada.")

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
