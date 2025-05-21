import csv
import sqlite3

def csv_para_sqlite(csv_file, db_file, tabela, colunas, ufs_nordeste):
    try:
        
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {tabela} (
                {', '.join([col + ' TEXT' for col in colunas])}
            );
        ''')

        with open(csv_file, mode='r', encoding='utf-8') as arquivo_csv:
            leitor_csv = csv.DictReader(arquivo_csv)

            inseridos = 0
            for linha in leitor_csv:
                if linha["SG_UF"] in ufs_nordeste:
                    valores = [linha[col] for col in colunas]
                    cursor.execute(
                        f'''
                        INSERT INTO {tabela} ({', '.join(colunas)})
                        VALUES ({', '.join(['?'] * len(colunas))})
                        ''',
                        valores
                    )
                    inseridos += 1

        conn.commit()
        conn.close()
        print(f"✅ Inserção concluída! {inseridos} registros adicionados em '{db_file}' na tabela '{tabela}'.")

    except Exception as e:
        print(f"❌ Erro: {str(e)}")


colunas_selecionadas = [
    "NO_REGIAO", 
    "CO_REGIAO",         
    "NO_UF",              
    "SG_UF",
    "CO_UF",            
    "NO_MUNICIPIO",
    "CO_MUNICIPIO",       
    "NO_MESORREGIAO",     
    "NO_MICRORREGIAO",    
    "NO_ENTIDADE",
    "CO_ENTIDADE",
    "QT_MAT_BAS",
    "QT_MAT_INF",
    "QT_MAT_FUND",
    "QT_MAT_MED",
    "QT_MAT_EJA",
    "QT_MAT_EJA_FUND",
    "QT_MAT_ESP",
    "QT_MAT_BAS_EAD",
    "QT_MAT_FUND_INT",
    "QT_MAT_MED_INT",
]

ufs_nordeste = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]

# Executa
csv_para_sqlite(
    csv_file="microdados_utf8.csv",
    db_file="dados_nordeste.db",
    tabela="instituicoes",
    colunas=colunas_selecionadas,
    ufs_nordeste=ufs_nordeste
)
