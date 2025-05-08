from flask import Flask, request, jsonify
import sqlite3

from models.InstituicaoEnsino import InstituicaoEnsino

app = Flask(__name__)

@app.route("/")
def index():
    versao = {"versao": "0.0.1"}
    return jsonify(versao), 200

@app.get("/instituicoes")
def instituicoesResource():
    print("Get - Instituições")

    try:
        instituicoesEnsino = []

        # Pegando os parâmetros da requisição (page e per_page)
        page = int(request.args.get('page', 1))  # Página padrão: 1
        per_page = int(request.args.get('per_page', 50))  # Número padrão de registros por página
        offset = (page - 1) * per_page  # Calcula o deslocamento

        with sqlite3.connect('dados_nordeste.db') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT NO_ENTIDADE, CO_ENTIDADE, QT_MAT_BAS 
                FROM instituicoes 
                LIMIT ? OFFSET ?
            """, (per_page, offset))
            resultSet = cursor.fetchall()

            for row in resultSet:
                no_entidade, co_entidade, qt_mat_bas = row
                instituicaoEnsino = {
                    "no_entidade": no_entidade,
                    "co_entidade": co_entidade,
                    "qt_mat_bas": qt_mat_bas
                }
                instituicoesEnsino.append(instituicaoEnsino)

    except sqlite3.Error as e:
        print(f"Erro no banco de dados: {e}")
        return jsonify({"mensagem": "Problema com o banco de dados."}), 500

    return jsonify({
        "page": page,
        "per_page": per_page,
        "total_registros": len(instituicoesEnsino),
        "dados": instituicoesEnsino
    }), 200

def validarInstituicao(content):
    if not isinstance(content, dict):
        return False
    if len(content.get('NO_ENTIDADE', '')) < 3 or content['NO_ENTIDADE'].isdigit():
        return False
    if not content.get('CO_ENTIDADE', '').isdigit():
        return False
    if not content.get('QT_MAT_BAS', '').isdigit():
        return False
    return True

@app.post("/instituicoes")
def instituicaoInsercaoResource():
    print("Post - Instituição")
    instituicaoJson = request.get_json()

    if validarInstituicao(instituicaoJson):
        no_entidade = instituicaoJson['NO_ENTIDADE']
        co_entidade = instituicaoJson['CO_ENTIDADE']
        qt_mat_bas = instituicaoJson['QT_MAT_BAS']

        with sqlite3.connect('dados_nordeste.db') as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO instituicoes (NO_ENTIDADE, CO_ENTIDADE, QT_MAT_BAS) VALUES(?, ?, ?)',
                (no_entidade, co_entidade, qt_mat_bas))
            conn.commit()
            id = cursor.lastrowid

        instituicaoEnsino = InstituicaoEnsino(id, no_entidade, co_entidade, qt_mat_bas)
        return jsonify(instituicaoEnsino.toDict()), 201

    return jsonify({"mensagem": "Dados inválidos"}), 400

@app.route("/instituicoes/<int:co_entidade>", methods=["DELETE"])
def instituicaoRemocaoResource(co_entidade):
    try:
        with sqlite3.connect('dados_nordeste.db') as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM instituicoes WHERE CO_ENTIDADE = ?", (co_entidade,))
            conn.commit()

            if cursor.rowcount > 0:
                return jsonify({"mensagem": "Instituição removida"}), 200
            return jsonify({"mensagem": "Instituição não encontrada"}), 404
    except sqlite3.Error as e:
        print(f"Erro no banco de dados: {e}")
        return jsonify({"mensagem": "Problema com o banco de dados."}), 500

@app.route("/instituicoes/<int:co_entidade>", methods=["PUT"])
def instituicaoAtualizacaoResource(co_entidade):
    print("Put - Instituição")
    instituicaoJson = request.get_json()

    if not validarInstituicao(instituicaoJson):
        return jsonify({"mensagem": "Dados inválidos"}), 400

    with sqlite3.connect('dados_nordeste.db') as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE instituicoes 
            SET NO_ENTIDADE = ?, QT_MAT_BAS = ?
            WHERE CO_ENTIDADE = ?
        """, (instituicaoJson["NO_ENTIDADE"], instituicaoJson["QT_MAT_BAS"], co_entidade))
        conn.commit()

        if cursor.rowcount > 0:
            return jsonify({"mensagem": "Instituição atualizada"}), 200
        return jsonify({"mensagem": "Instituição não encontrada"}), 404

@app.route("/instituicoes/<int:co_entidade>", methods=["GET"])
def instituicoesByIdResource(co_entidade):
    try:
        with sqlite3.connect('dados_nordeste.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT NO_ENTIDADE, CO_ENTIDADE, QT_MAT_BAS FROM instituicoes WHERE CO_ENTIDADE = ?", (co_entidade,))
            row = cursor.fetchone()

            if row:
                no_entidade, co_entidade, qt_mat_bas = row
                instituicaoEnsino = {
                    "no_entidade": no_entidade,
                    "co_entidade": co_entidade,
                    "qt_mat_bas": qt_mat_bas
                }
                return jsonify(instituicaoEnsino), 200

            return jsonify({"mensagem": "Instituição não encontrada"}), 404

    except sqlite3.Error as e:
        print(f"Erro no banco de dados: {e}")
        return jsonify({"mensagem": "Problema com o banco de dados."}), 500

if __name__ == '__main__':
    app.run(debug=True)
