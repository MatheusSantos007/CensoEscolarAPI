import sqlite3
import requests

def api_estados_nordeste(api_url, db_file, tabela):
    try:
        # Conectar ao banco
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Criar tabela
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {tabela} (
                id INTEGER PRIMARY KEY,
                sigla TEXT,
                nome TEXT,
                regiao_id INTEGER,
                regiao_sigla TEXT,
                regiao_nome TEXT
            );
        ''')

       
        resposta = requests.get(api_url)
        if resposta.status_code == 200:
            estados = resposta.json()  
            
            inseridos = 0
            for estado in estados:  
                valores = (
                    estado["id"], estado["sigla"], estado["nome"],
                    estado["regiao"]["id"], estado["regiao"]["sigla"], estado["regiao"]["nome"]
                )
                cursor.execute(f'''
                    INSERT INTO {tabela} (id, sigla, nome, regiao_id, regiao_sigla, regiao_nome)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', valores)
                inseridos += 1

            conn.commit()
            print(f"✅ Inserção concluída! {inseridos} estados do Nordeste adicionados na tabela '{tabela}'.")

        else:
            print(f"❌ Erro ao acessar API: {resposta.status_code}")

        conn.close()

    except Exception as e:
        print(f"❌ Erro: {str(e)}")



api_estados_nordeste(
    api_url="https://servicodados.ibge.gov.br/api/v1/localidades/regioes/2/estados",
    db_file="dados_nordeste.db",
    tabela="UFs"
)
