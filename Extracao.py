import os,requests
import pandas as pd     

#os.mkdir('extracao')
diretorio_base = os.getcwd()
nome_pasta_extracao = "extracao"
nome_pasta_saida = "saida"
pasta_extracao = os.path.join(diretorio_base, nome_pasta_extracao)
pasta_extracao_saida = os.path.join(diretorio_base, nome_pasta_saida)


class Criacaodepastas:
    def Criar_pasta_extracao():
        if not os.path.exists(pasta_extracao):
            os.makedirs(pasta_extracao)
            print(f"A pasta '{pasta_extracao}' criada.")
        else:
            print(f"A pasta '{pasta_extracao}' existe.")
            
    def Criar_pasta_saida():
        if not os.path.exists(pasta_extracao_saida):
            os.makedirs(pasta_extracao_saida)
            print(f"A pasta '{pasta_extracao_saida}' criada.")
        else:
            print(f"A pasta '{pasta_extracao_saida}' existe.")

class Buscardados:

    def Url_dos_dados(): #URl dos dados
        url = {
            'constructors.csv':'https://github.com/CaioSobreira/dti_arquivos/raw/main/constructors.csv',
            'drivers.csv':'https://github.com/CaioSobreira/dti_arquivos/raw/main/drivers.csv',
            'races.csv': 'https://github.com/CaioSobreira/dti_arquivos/raw/main/races.csv',
            'results.csv': 'https://github.com/CaioSobreira/dti_arquivos/raw/main/results.csv'}
        return url
     

    def Busca_dados(self):
        arquivo = {}
        for nome_arquivo, url in Buscardados.Url_dos_dados().items():
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    arquivo[nome_arquivo] = response.content
                    print(f"Extração realizada: {nome_arquivo}")
                else:
                    print(f"Erro ao baixar {nome_arquivo}: Status {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar {url}: {e}")
        return arquivo

    def Salvar_arquivos(dados_baixados):
        for nome_arquivo, conteudo in dados_baixados.items():
            caminho_arquivo = os.path.join(pasta_extracao, nome_arquivo)
            with open(caminho_arquivo, "wb") as f:
                f.write(conteudo)
            print(f"Arquivo salvo: {caminho_arquivo}")

class Tratamentoarquivo: 
    
    def __init__(self):
        self.pastaSaida = os.path.join(os.getcwd(), "saida")
        self.pasta_extracao = os.path.join(diretorio_base, nome_pasta_extracao)

    
    def Converter_para_xlsx(self):
        pasta_extracao_path = os.path.join(os.getcwd(), 'extracao')
        
        for nome_arquivo in os.listdir(pasta_extracao_path):
            if nome_arquivo.endswith(".csv"):
                caminho_csv = os.path.join(pasta_extracao_path, nome_arquivo)
                nome_sem_extensao = os.path.splitext(nome_arquivo)[0]
                caminho_excel = os.path.join(self.pastaSaida, f"{nome_sem_extensao}.xlsx")
                try:
                    df = pd.read_csv(caminho_csv, encoding='utf-8', sep=None, engine='python')
                    df.to_excel(caminho_excel, index=False)
                    print(f"Convertido: {nome_arquivo} para {caminho_excel}")
                except Exception as e:
                    print(f"Erro ao converter {nome_arquivo}: {e}")
    
    def tratar_arquivos(self):
        for file in os.listdir(self.pasta_extracao):
            
            if file == "constructors.csv":
                #nome_arquivo = "constructors.csv"
                caminho_csv = os.path.join(self.pasta_extracao, file)
                caminho_excel = os.path.join(self.pastaSaida, "constructors.xlsx")

                try:
                    df = pd.read_csv(caminho_csv, encoding='utf-8', sep=None, engine='python')
                    colunas_desejadas = ["constructorId", "name", "nationality"]
                    df = df[colunas_desejadas].rename(columns={
                        "constructorId": "montadora_id",
                        "name": "nome",
                        "nationality": "nacionalidade"
                    })
                    df.to_excel(caminho_excel, index=False)
                    print(f"Arquivo tratado e salvo em: {caminho_excel}")

                except Exception as e:
                    print(f"Erro ao tratar {file}: {e}") 
                    
            elif file == "drivers.csv":
                caminho_csv = os.path.join(self.pasta_extracao, file)
                caminho_excel = os.path.join(self.pastaSaida, "drivers.xlsx")

                try:
                    df = pd.read_csv(caminho_csv, encoding='utf-8', sep=None, engine='python')
                    df["nome_completo"] = df["forename"] + " " + df["surname"]

                    # Selecionar e renomear colunas
                    colunas_desejadas = ["driverId", "nome_completo", "nationality"]
                    df = df[colunas_desejadas].rename(columns={
                        "driverId": "piloto_id",
                        "nationality": "nacionalidade"
                    })
                    df.info()
                    # Salvar como Excel
                    df.to_excel(caminho_excel, index=False)
                    print(f"Arquivo tratado e salvo em: {caminho_excel}")

                except Exception as e:
                    print(f"Erro ao tratar {file}: {e}")
            
            elif file == "races.csv":
                caminho_csv = os.path.join(self.pasta_extracao, file)
                caminho_excel = os.path.join(self.pastaSaida, "races.xlsx")

                try:
                    df = pd.read_csv(caminho_csv, encoding='utf-8', sep=None, engine='python')

                    # Selecionar e renomear colunas
                    colunas_desejadas = ["raceId", "year", "name", "date"]
                    df = df[colunas_desejadas].rename(columns={
                        "raceId": "corrida_id",
                        "year": "ano",
                        "name": "nome",
                        "date": "corrida_data"
                    })
                    df["corrida_data"] = pd.to_datetime(df["corrida_data"], errors='coerce')
                    df.to_excel(caminho_excel, index=False)
                    print(f"Arquivo tratado e salvo em: {caminho_excel}")

                except Exception as e:
                    print(f"Erro ao tratar {file}: {e}")
            
            elif file == "results.csv":
                caminho_csv = os.path.join(self.pasta_extracao, file)
                caminho_excel = os.path.join(self.pastaSaida, "results.xlsx")

                try:
                    df = pd.read_csv(caminho_csv, encoding='utf-8', sep=None, engine='python')
                    colunas_desejadas = [
                        "resultId", "raceId", "driverId",
                        "constructorId", "positionOrder",
                        "points", "fastestLapTime"
                    ]
                    df = df[colunas_desejadas].rename(columns={
                        "resultId": "resultado_id",
                        "raceId": "corrida_id",
                        "driverId": "piloto_id",
                        "constructorId": "montadora_id",
                        "positionOrder": "posicao_ordem",
                        "points": "pontos",
                        "fastestLapTime": "volta_mais_rapida_tempo"
                    })

                    df.to_excel(caminho_excel, index=False)
                    print(f"Arquivo tratado e salvo em: {caminho_excel}")

                except Exception as e:
                    print(f"Erro ao tratar {file}: {e}")
                    
            else:
                print('Sem arquivos na pasta')
                break


    
if __name__ == '__main__':
  Tratamentoarquivo
  Buscardados
  Criacaodepastas  

