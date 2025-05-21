import sqlite3
import requests

def api_municipios_sqlite(api_url, db_file, tabela):
    try:
        
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {tabela} (
                id INTEGER PRIMARY KEY,
                nome TEXT,
                microrregiao_id INTEGER,
                microrregiao_nome TEXT,
                mesorregiao_id INTEGER,
                mesorregiao_nome TEXT,
                uf_id INTEGER,
                uf_sigla TEXT,
                uf_nome TEXT,
                regiao_id INTEGER,
                regiao_sigla TEXT,
                regiao_nome TEXT
            );
        ''')

        
        resposta = requests.get(api_url)
        if resposta.status_code == 200:
            municipios = resposta.json()
            
            inseridos = 0
            for municipio in municipios:
                valores = (
                    municipio["id"], municipio["nome"],
                    municipio["microrregiao"]["id"], municipio["microrregiao"]["nome"],
                    municipio["microrregiao"]["mesorregiao"]["id"], municipio["microrregiao"]["mesorregiao"]["nome"],
                    municipio["microrregiao"]["mesorregiao"]["UF"]["id"],
                    municipio["microrregiao"]["mesorregiao"]["UF"]["sigla"],
                    municipio["microrregiao"]["mesorregiao"]["UF"]["nome"],
                    municipio["microrregiao"]["mesorregiao"]["UF"]["regiao"]["id"],
                    municipio["microrregiao"]["mesorregiao"]["UF"]["regiao"]["sigla"],
                    municipio["microrregiao"]["mesorregiao"]["UF"]["regiao"]["nome"]
                )

                cursor.execute(f'''
                    INSERT INTO {tabela} (id, nome, microrregiao_id, microrregiao_nome, mesorregiao_id, mesorregiao_nome, uf_id, uf_sigla, uf_nome, regiao_id, regiao_sigla, regiao_nome)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', valores)
                inseridos += 1

            conn.commit()
            print(f"✅ Inserção concluída! {inseridos} municípios adicionados na tabela '{tabela}'.")

        else:
            print(f"❌ Erro ao acessar API: {resposta.status_code}")

        conn.close()

    except Exception as e:
        print(f"❌ Erro: {str(e)}")



api_municipios_sqlite(
    api_url="https://servicodados.ibge.gov.br/api/v1/localidades/regioes/2/municipios",
    db_file="dados_nordeste.db",  
    tabela="municipios"
)
