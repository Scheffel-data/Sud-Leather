
# --- Configurações do BigQuery ---
# SUBSTITUA PELOS SEUS VALORES REAIS:
PROJECT_ID = "sud-leather"  # Ex: "my-gcp-project-12345"
DATASET_ID = "Data_base"  # Ex: "dados_nfe"
TABLE_ID = "Frigorifico_Nota_Fiscal"      # Ex: "notas_fiscais"

# Inicializa os clientes do Cloud Storage e BigQuery
storage_client = storage.Client()
bigquery_client = bigquery.Client(project=PROJECT_ID)

# --- Sua função de parsing XML (com as últimas melhorias) ---
def criar_df_nfe(xml_content):
    """
    Extrai dados específicos de um conteúdo XML de NF-e e os retorna em um DataFrame do Pandas.

    Args:
        xml_content (str): O conteúdo do arquivo XML da NF-e como string.

    Returns:
        pandas.DataFrame: Um DataFrame contendo os dados extraídos da NF-e.
    """
    try:
        # Define o namespace para a busca dos elementos
        namespaces = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

        # Analisa o conteúdo XML (não mais um caminho de arquivo)
        root = ET.fromstring(xml_content)

        # --- Extrai informações que são únicas por nota ---
        infNFe = root.find('.//nfe:infNFe', namespaces)
        
        # Extrai o número da NF-e
        numero_nf_element = infNFe.find('.//nfe:ide/nfe:nNF', namespaces)
        numero_nf = numero_nf_element.text if numero_nf_element is not None else None

        # Extrai a data de emissão (tenta dhEmi primeiro, senão dEmi)
        data_emissao_element = infNFe.find('.//nfe:ide/nfe:dhEmi', namespaces)
        if data_emissao_element is None: # Se não encontrou dhEmi, tenta dEmi
            data_emissao_element = infNFe.find('.//nfe:ide/nfe:dEmi', namespaces)
            
        data_emissao_str = data_emissao_element.text if data_emissao_element is not None else None
        
        # Formata a data se ela foi encontrada
        data_emissao_formatada = None
        if data_emissao_str:
            try:
                # Tenta parser com fuso horário (se for dhEmi)
                if 'T' in data_emissao_str and ('+' in data_emissao_str or '-' == data_emissao_str[-6]):
                    data_emissao_formatada = datetime.fromisoformat(data_emissao_str).date()
                # Tenta parser sem fuso horário (se for dhEmi sem fuso ou dEmi)
                elif 'T' in data_emissao_str:
                    data_emissao_formatada = datetime.strptime(data_emissao_str.split('T')[0], '%Y-%m-%d').date()
                else: # Se for apenas a data (dEmi)
                    data_emissao_formatada = datetime.strptime(data_emissao_str, '%Y-%m-%d').date()
            except ValueError:
                print(f"Aviso: Não foi possível formatar a data de emissão '{data_emissao_str}'. Será armazenada como string.")
                data_emissao_formatada = data_emissao_str # Armazena como string se der erro de formatação


        emitente_nome = infNFe.find('.//nfe:emit/nfe:xNome', namespaces).text if infNFe.find('.//nfe:emit/nfe:xNome', namespaces) is not None else None
        emitente_cnpj = infNFe.find('.//nfe:emit/nfe:CNPJ', namespaces).text if infNFe.find('.//nfe:emit/nfe:CNPJ', namespaces) is not None else None
        
        # Extrai a quantidade de volumes (peças)
        qvol_element = infNFe.find('.//nfe:transp/nfe:vol/nfe:qVol', namespaces)
        quantidade_pecas = int(float(qvol_element.text)) if qvol_element is not None and qvol_element.text else 0

        # --- Itera sobre cada produto na nota para criar uma lista de registros ---
        lista_produtos = []
        for det in infNFe.findall('.//nfe:det', namespaces):
            # Adicionado .text e verificação None para evitar erros caso a tag não exista
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
            
        # Cria o DataFrame a partir da lista de dicionários
        df = pd.DataFrame(lista_produtos)
        return df

    except Exception as e:
        print(f"Ocorreu um erro ao processar o conteúdo XML: {e}")
        return None

# --- Função principal da Cloud Function ---
@functions_framework.cloud_event
def process_nfe_xml(cloud_event):
    """
    Função da Google Cloud Function que é acionada por eventos do Cloud Storage.
    Processa arquivos XML de NF-e e insere os dados no BigQuery,
    e então move o arquivo para a pasta 'processados'.
    """
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"] # Caminho completo do arquivo, ex: recebidas/NFE-123.xml

    # Verifica se o arquivo é um XML e está na pasta "recebidas"
    if not file_name.lower().endswith('.xml') or not file_name.startswith('recebidas/'):
        print(f"Arquivo {file_name} não é um XML ou não está na pasta 'recebidas'. Ignorando.")
        return

    print(f"Iniciando processamento do arquivo: {file_name} do bucket: {bucket_name}")

    try:
        # Baixa o conteúdo do arquivo XML do Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        xml_content = blob.download_as_text()

        # Chama sua função para criar o DataFrame a partir do conteúdo XML
        df_nfe = criar_df_nfe(xml_content)

        if df_nfe is not None and not df_nfe.empty:
            # Converte o DataFrame para uma lista de dicionários (formato para BigQuery)
            rows_to_insert = df_nfe.to_dict(orient='records')

            # Insere os dados no BigQuery
            table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
            
            errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

            if errors == []:
                print(f"Dados do arquivo {file_name} inseridos no BigQuery com sucesso.")
                
                # --- Lógica para mover o arquivo após o sucesso ---
                # Pega a data atual para a estrutura de pasta (ano/mes)
                current_date = datetime.now()
                # Formata a nova pasta de destino
                destination_folder = f"processados/{current_date.year:04d}/{current_date.month:02d}"
                # Extrai apenas o nome do arquivo, ex: NFE-123.xml
                base_file_name = file_name.split('/')[-1]
                new_file_path = f"{destination_folder}/{base_file_name}"
                
                # Cria um novo blob no destino
                new_blob = bucket.blob(new_file_path)
                
                # Copia o conteúdo do blob original para o novo blob
                blob.rewrite(new_blob)
                
                # Apaga o blob original
                blob.delete()
                
                print(f"Arquivo {file_name} movido para {new_file_path} com sucesso.")

            else:
                print(f"Erros ao inserir dados do arquivo {file_name} no BigQuery: {errors}")
                # --- Lógica para mover para pasta de ERROS se houver falha na inserção ---
                error_folder = "erros_bigquery"
                base_file_name = file_name.split('/')[-1]
                error_file_path = f"{error_folder}/{base_file_name}"

                error_blob = bucket.blob(error_file_path)
                blob.rewrite(error_blob)
                blob.delete()
                print(f"Arquivo {file_name} movido para {error_file_path} devido a erros de BigQuery.")
                
        else:
            print(f"Nenhum dado válido extraído do arquivo {file_name}.")
            # --- Lógica para mover para pasta de ERROS se não houver dados válidos ---
            error_folder = "erros_parsing"
            base_file_name = file_name.split('/')[-1]
            error_file_path = f"{error_folder}/{base_file_name}"

            error_blob = bucket.blob(error_file_path)
            blob.rewrite(error_blob)
            blob.delete()
            print(f"Arquivo {file_name} movido para {error_file_path} devido a falha no parsing/extração de dados.")
            
    except Exception as e:
        print(f"Erro crítico ao processar o arquivo {file_name}: {e}")
        # --- Lógica para mover para pasta de ERROS para qualquer outra exceção ---
        error_folder = "erros_gerais"
        base_file_name = file_name.split('/')[-1]
        error_file_path = f"{error_folder}/{base_file_name}"

        # Tenta mover, mas se o erro foi no download, o blob pode não existir
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            error_blob = bucket.blob(error_file_path)
            blob.rewrite(error_blob)
            blob.delete()
            print(f"Arquivo {file_name} movido para {error_file_path} devido a erro crítico.")
        except Exception as move_e:
            print(f"Não foi possível mover o arquivo {file_name} para a pasta de erros: {move_e}")