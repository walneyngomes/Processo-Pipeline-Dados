import sqlite3

class DataBaseRepository:
    def get_area_colhida(self,municipio, ano):
        conn = sqlite3.connect('/home/wa/base_gold_ibge.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT Valor_q FROM tabela_final_gold WHERE Variavel_q= 'Área colhida' AND Cidade = ? AND Ano_q = ?",
            (municipio, ano)
        )
        area_colhida = cursor.fetchone()
        conn.close()

        return area_colhida[0] if area_colhida else None

    def get_produtividade(self,  ano, estados=[]):
        marcadores_estados = ','.join('?' for _ in estados)
        query = f"SELECT Produtividade FROM Produtividade_Estado_Ano WHERE Estado IN ({marcadores_estados}) AND Ano = {ano}"

        conn = sqlite3.connect('/home/wa/base_gold_ibge.db')
        cursor = conn.cursor()
        cursor.execute(
            query,
            estados  # Adicione uma vírgula para torná-lo uma tupla
        )
        resultado = cursor.fetchall()
        conn.close()
        return resultado


    def get_obter_quantidade_dados_produzida(self,  anos=[], cidades=[]):
        marcadores_cidades = ','.join('?' for _ in cidades)
        marcadores_anos = ','.join('?' for _ in anos)
        query = f"SELECT Valor_q FROM tabela_final_gold WHERE Variavel_q= 'Quantidade produzida' AND Cidade IN ({marcadores_cidades}) AND Ano_q IN( {marcadores_anos})"
        conn = sqlite3.connect('/home/wa/base_gold_ibge.db')
        cursor = conn.cursor()
        cursor.execute(
            query,
             cidades+anos
        )
        resultado = cursor.fetchall()
        conn.close()
        return resultado

