import requests
import sqlite3

API_IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes/2/municipios"

def obter_microrregioes_nordeste():
    response = requests.get(API_IBGE_MUNICIPIOS)
    microrregioes = {}

    if response.status_code == 200:
        for municipio in response.json():
            microrregiao = municipio.get("microrregiao")
            mesorregiao = microrregiao.get("mesorregiao") if microrregiao else None
            uf = mesorregiao.get("UF") if mesorregiao else None
            regiao = uf.get("regiao") if uf else None

            if regiao and regiao["id"] == 2:  # Nordeste
                microrregioes[microrregiao["id"]] = {
                    "id": microrregiao["id"],
                    "nome": microrregiao["nome"],
                    "mesorregiao_id": mesorregiao["id"]
                }

    return list(microrregioes.values())

def salvar_microrregioes(db_file):
    microrregioes = obter_microrregioes_nordeste()

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS microrregiao (
                id INTEGER PRIMARY KEY,
                nome TEXT,
                mesorregiao_id INTEGER,
                FOREIGN KEY (mesorregiao_id) REFERENCES mesorregiao(id)
            );
        """)

        for micro in microrregioes:
            cursor.execute("""
                INSERT OR IGNORE INTO microrregiao (id, nome, mesorregiao_id) VALUES (?, ?, ?)
            """, (micro["id"], micro["nome"], micro["mesorregiao_id"]))

        conn.commit()
        print("✅ Microrregiões salvas no banco!")

# Chamada principal
salvar_microrregioes("dados_nordeste.db")
