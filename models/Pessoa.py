class Pessoa:
    def __init__(self, nome, idade):
        self.nome = nome
        self.idade = idade

    def toDict(self):
        return {"nome": self.nome, "idade": self.idade}
