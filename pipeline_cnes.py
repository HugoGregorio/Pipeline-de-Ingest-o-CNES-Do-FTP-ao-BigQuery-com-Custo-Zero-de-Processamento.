import os
import ftplib
import zipfile
import pandas as pd
import pandas_gbq
from tqdm import tqdm
from google.colab import auth

# --- CONFIGURAÇÕES DO GOOGLE CLOUD ---
# Substitua as variáveis abaixo pelos dados do seu próprio ambiente GCP
PROJECT_ID = 'seu-project-id-aqui' 
DATASET_TABLE = 'seu_dataset.sua_tabela_destino'

# 1. Conexão com FTP do DATASUS e Download
print("Conectando ao FTP do DATASUS...")
try:
    ftp = ftplib.FTP('ftp.datasus.gov.br', timeout=120)
    ftp.login() 
    ftp.cwd('/cnes/') 

    # Detecção dinâmica do arquivo mais recente (Future-Proof)
    files = ftp.nlst()
    base_files = [f for f in files if f.startswith('BASE_DE_DADOS_CNES_') and f.endswith('.ZIP')]
    base_files.sort(reverse=True) 
    latest_file = base_files[0]
    print(f"Arquivo mais recente encontrado: {latest_file}")

    local_zip = f"/content/{latest_file}"
    print(f"Iniciando o download de {latest_file}...")
    
    file_size = ftp.size(latest_file)
    with open(local_zip, 'wb') as f:
        with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024, desc="Baixando") as pbar:
            def callback(data):
                f.write(data)
                pbar.update(len(data))
            ftp.retrbinary(f"RETR {latest_file}", callback)
            
    ftp.quit()
    print("\nDownload concluído!")
except Exception as e:
    # Fallback de segurança caso o FTP sofra timeout mas o arquivo já esteja em disco
    print(f"\nAviso FTP: {e}. Prosseguindo com o arquivo local se já existir.")
    local_zip = "/content
