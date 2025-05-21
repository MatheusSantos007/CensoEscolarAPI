from flask import Flask, request, jsonify
import sqlite3
from marshmallow import Schema, fields, validate, ValidationError

from utils import get_db_connection, validate_data


app = Flask(__name__)
conn = get_db_connection()

# --- Schemas Marshmallow para validação ---

class InstituicaoSchema(Schema):
    NO_ENTIDADE = fields.Str(required=True, validate=validate.Length(min=3))
    CO_ENTIDADE = fields.Int(required=True)
    QT_MAT_BAS = fields.Int(required=True)

instituicao_schema = InstituicaoSchema()

# --- ROTAS ---

# ===== INSTITUIÇÕES =====

@app.get("/instituicoes")
def get_instituicoes():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT NO_ENTIDADE, CO_ENTIDADE, QT_MAT_BAS FROM instituicoes LIMIT ? OFFSET ?", (per_page, offset))
    rows = cursor.fetchall()
    conn.close()

    instituicoes = [dict(row) for row in rows]
    return jsonify({
        "page": page,
        "per_page": per_page,
        "dados": instituicoes
    }), 200

@app.post("/instituicoes")
def post_instituicao():
    data = request.get_json()
    validated, errors = validate_data(InstituicaoSchema(), data)
    if errors:
        return jsonify({"mensagem": "Dados inválidos", "erros": errors}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO instituicoes (NO_ENTIDADE, CO_ENTIDADE, QT_MAT_BAS) VALUES (?, ?, ?)",
            (validated['NO_ENTIDADE'], validated['CO_ENTIDADE'], validated['QT_MAT_BAS'])
        )
        conn.commit()
        conn.close()
        return jsonify({"mensagem": "Instituição inserida"}), 201
    except sqlite3.IntegrityError as e:
        conn.close()
        return jsonify({"mensagem": "Erro ao inserir instituição", "erro": str(e)}), 400

@app.put("/instituicoes/<int:co_entidade>")
def put_instituicao(co_entidade):
    data = request.get_json()
    # For update, CO_ENTIDADE in payload can be optional or must match URL
    if 'CO_ENTIDADE' in data and data['CO_ENTIDADE'] != co_entidade:
        return jsonify({"mensagem": "CO_ENTIDADE no corpo não corresponde ao da URL"}), 400
    data['CO_ENTIDADE'] = co_entidade
    validated, errors = validate_data(InstituicaoSchema(), data)
    if errors:
        return jsonify({"mensagem": "Dados inválidos", "erros": errors}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE instituicoes 
        SET NO_ENTIDADE = ?, QT_MAT_BAS = ?
        WHERE CO_ENTIDADE = ?
    """, (validated['NO_ENTIDADE'], validated['QT_MAT_BAS'], co_entidade))
    conn.commit()
    updated = cursor.rowcount
    conn.close()

    if updated == 0:
        return jsonify({"mensagem": "Instituição não encontrada"}), 404
    return jsonify({"mensagem": "Instituição atualizada"}), 200

@app.delete("/instituicoes/<int:co_entidade>")
def delete_instituicao(co_entidade):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM instituicoes WHERE CO_ENTIDADE = ?", (co_entidade,))
    conn.commit()
    deleted = cursor.rowcount
    conn.close()
    if deleted == 0:
        return jsonify({"mensagem": "Instituição não encontrada"}), 404
    return jsonify({"mensagem": "Instituição deletada"}), 200

if __name__ == "__main__":
    app.run(debug=True)

