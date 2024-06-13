from repository import DataBaseRepository
class AreaColhidaService:
    def __init__(self):
        self.repo = DataBaseRepository()


    def get_area_colhida(self, municipio, ano):
        return self.repo.get_area_colhida(municipio, ano)

    def get_produtividade(self,ano,estados=[]):
        return self.repo.get_produtividade(ano,estados)


    def get_quantidade_produzida(self,municipios=[], anos=[]):
        dados= self.repo.get_obter_quantidade_dados_produzida(anos, municipios)

        if(len(dados)<=100):
            return dados

        return[('nenhum dado poderá ser disponibilizado. Verifique o seu pacote')]


