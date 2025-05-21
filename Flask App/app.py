from flask import Flask, jsonify, request, abort
import json

app = Flask(__name__)


with open("dados_paraiba.json", "r", encoding="utf-8") as f:
    instituicoes = json.load(f)

@app.get("/instituicoesensino")
def listar_instituicoes():
    return jsonify(instituicoes)

@app.get("/instituicoesensino/<co_entidade>")
def obter_instituicao(co_entidade):
    
    co_entidade = str(co_entidade)  

    for inst in instituicoes:
        if str(inst["CO_ENTIDADE"]) == co_entidade:  
            return jsonify(inst)

    abort(404, description="Instituição não encontrada.")

@app.post("/instituicoesensino")
def adicionar_instituicao():
    nova_instituicao = request.get_json()
    if not nova_instituicao or "CO_ENTIDADE" not in nova_instituicao:
        abort(400, description="Dados inválidos ou campo 'CO_ENTIDADE' ausente.")
    
    for inst in instituicoes:
        if inst["CO_ENTIDADE"] == nova_instituicao["CO_ENTIDADE"]:
            abort(409, description="Instituição já existe.")

    instituicoes.append(nova_instituicao)
    return jsonify(nova_instituicao), 201

@app.put("/instituicoesensino")
def atualizar_instituicao():
    dados = request.get_json()
    if not dados or "CO_ENTIDADE" not in dados:
        abort(400, description="Dados inválidos ou campo 'CO_ENTIDADE' ausente.")
    
    for idx, inst in enumerate(instituicoes):
        if inst["CO_ENTIDADE"] == dados["CO_ENTIDADE"]:
            instituicoes[idx] = dados
            return jsonify(dados)

    abort(404, description="Instituição não encontrada.")

@app.delete("/instituicoesensino/<co_entidade>")
def deletar_instituicao(co_entidade):
    
    co_entidade = str(co_entidade)  

    for inst in instituicoes:
        if str(inst["CO_ENTIDADE"]) == co_entidade: 
            instituicoes.remove(inst)
            return jsonify({"mensagem": "Instituição removida com sucesso."})

    abort(404, description="Instituição não encontrada.")

if __name__ == "__main__":
    app.run(debug=True)