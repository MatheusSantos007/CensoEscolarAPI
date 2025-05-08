from models.Pessoa import Pessoa

class Aluno(Pessoa):
    def __init__(self, nome, idade, matricula):
        super().__init__(nome, idade)
        self.matricula = matricula

    def toDict(self):
        data = super().toDict()
        data["matricula"] = self.matricula
        return data
