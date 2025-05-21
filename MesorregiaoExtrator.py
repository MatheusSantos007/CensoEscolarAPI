import requests
import sqlite3

API_IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes/2/municipios"
def obter_mesorregioes_nordeste():
    response = requests.get(API_IBGE_MUNICIPIOS)
    mesorregioes = {}

    if response.status_code == 200:
        for municipio in response.json():
            microrregiao = municipio.get("microrregiao")
            mesorregiao = microrregiao.get("mesorregiao") if microrregiao else None
            uf = mesorregiao.get("UF") if mesorregiao else None
            regiao = uf.get("regiao") if uf else None

            if regiao and regiao["id"] == 2:  # Nordeste
                mesorregioes[mesorregiao["id"]] = {
                    "id": mesorregiao["id"],
                    "nome": mesorregiao["nome"],
                    "uf_id": uf["id"]
                }

    return list(mesorregioes.values())

def salvar_mesorregioes(db_file):
    mesorregioes = obter_mesorregioes_nordeste()

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mesorregiao (
                id INTEGER PRIMARY KEY,
                nome TEXT,
                uf_id INTEGER,
                FOREIGN KEY (uf_id) REFERENCES uf(id)
            );
        """)

        for meso in mesorregioes:
            cursor.execute("""
                INSERT OR IGNORE INTO mesorregiao (id, nome, uf_id) VALUES (?, ?, ?)
            """, (meso["id"], meso["nome"], meso["uf_id"]))

        conn.commit()
        print("✅ Mesorregiões salvas no banco!")

# Chamada principal
salvar_mesorregioes("dados_nordeste.db")
