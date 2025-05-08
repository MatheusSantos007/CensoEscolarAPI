class InstituicaoEnsino():
    def __init__(self, id, no_entidade, co_entidade, qt_mat_bas):
        self.id = id
        self.no_entidade = no_entidade
        self.co_entidade = co_entidade
        self.qt_mat_bas = qt_mat_bas

    def toDict(self):
        return {"id": self.id, "no_entidade": self.no_entidade, "co_entidade": self.co_entidade, "qt_mat_bas": self.qt_mat_bas}
