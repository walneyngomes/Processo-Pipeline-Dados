# SQLITE
import sqlite3
import os
from bronzer-camada import Extract

class SilverData:

    def __init__(self):

        self.extrair_dados = Extract().extracao_dados()
        print(f"Diretório atual: {os.getcwd()}")

        # fluxo para a tabela final
        print("""
            criando a tabela final 

        """)
        self.criar_tabela_final_gold()
        print("""
            criando a tabela final 

        """)
        self.carga_tabela_final_gold()
        self.criar_tabelaview_gold()
        print("""
            tabela padrao e view criadas com sucesso
        """)
        print(""" Realizando a carga de dados""")
        print("""Carga realizada com sucesso""")

    def criar_tabela_final_gold(self):
        conn = sqlite3.connect('base_gold_ibge.db')
        cursor = conn.cursor()
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS tabela_final_gold (            
                           Municipio_cod_q INTEGER,
                       Municipio_q VARCHAR(200),
                       Variavel_cod_q INTEGER,
                       Variavel_q VARCHAR(200),
                       Ano_cod_q INTEGER,
                       Ano_q INTEGER,
                       Produto_lavouras_temporarias_permanentes_cod_q INTEGER,
                       Produto_lavouras_temporarias_permanentes_q VARCHAR(200),
                       Unidade_Medida_cod_q VARCHAR(200),
                       Unidade_Medida_q VARCHAR(200),
                       Nivel_Territorial_cod_q VARCHAR(200),
                       Nivel_Territorial_q VARCHAR(200),
                       Valor_q VARCHAR(200),
                       Cidade VARCHAR(200),
                       nome_estado VARCHAR(200)
                )""")

        except sqlite3.Error as e:
            print(f"Tabela ja inserida. prosseguindo {e}")
        conn.commit()
        conn.close()

    def carga_tabela_final_gold(self):
        conn = sqlite3.connect('base_gold_ibge.db')
        cursor = conn.cursor()
        for i in self.extrair_dados.collect():
            cursor.execute("""INSERT INTO tabela_final_gold (
                       'Municipio_cod_q',
                       'Municipio_q',
                       'Variavel_cod_q',
                       'Variavel_q',
                       'Ano_cod_q',
                       'Ano_q',
                       'Produto_lavouras_temporarias_permanentes_cod_q',
                       'Produto_lavouras_temporarias_permanentes_q',
                       'Unidade_Medida_cod_q',
                       'Unidade_Medida_q',
                       'Nivel_Territorial_cod_q',
                       'Nivel_Territorial_q',
                       'Valor_q',
                       'Cidade',
                       'nome_estado' ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)""", (
            i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14]))
        conn.commit()
        conn.close()

    def criar_tabelaview_gold(self):
        conn = sqlite3.connect('base_gold_ibge.db')
        cursor = conn.cursor()
        try:
            cursor.execute("""CREATE VIEW Produtividade_Estado_Ano AS
    SELECT
        nome_estado AS Estado,
        Ano_q AS Ano,
        SUM(CASE WHEN Variavel_q = 'Quantidade produzida' THEN Valor_q END) / 
        SUM(CASE WHEN Variavel_q = 'Área colhida' THEN Valor_q END) AS Produtividade
    FROM tabela_final_gold
    GROUP BY nome_estado, Ano_q;""")
        except sqlite3.Error as e:
            print(f"Tabela ja inserida. prosseguindo {e}")

        conn.commit()
        conn.close()