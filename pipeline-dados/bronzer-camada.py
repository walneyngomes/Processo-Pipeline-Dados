# Para fazer solicitações HTTP
import requests
# Capturando dados do IBGE através de sua API
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, broadcast, col, split
import numpy as np
import datetime


class Extract:

    def obter_dados_por_ano(self, ano, numero="216"):
        # esse metodo é para obter todos os anos da area colhida e quantidade produzida
        # numero 216 é a area colhida e o 214 é o quantidade produzida.

        url = f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/{numero}/p/{ano}/c782/40124?formato=json"
        return requests.get(url).json()[1:]

    # Obtendo do ano 2018 ate o ano corrente
    # retornar os dados em forma de lista dos anos 2018...- atual
    def processar_ano_corrente(self):
        ano_corrente = datetime.datetime.now().year
        return np.arange(2018, ano_corrente + 1)

    def extracao_dados(self):
        spark = SparkSession.builder.appName("MinhaAplicacao").getOrCreate()
        # essa variavel chama a funcao que obtem os dados de 2018 ao atual
        # entao, ela é uma lista de anos
        anos = self.processar_ano_corrente()
        # aqui é realizado o RDD
        # Criando um RDD a partir da lista de anos
        rdd_anos = spark.sparkContext.parallelize(anos)
        # Usando a função obter_dados_por_ano em paralelo para obter os dados de todos os anos
        # Aplicando uma função lambda ao RDD para obter dados para cada ano
        # A função obter_dados_por_ano é chamada para cada ano com o número padrao 216 como argumento
        dados_por_ano = rdd_anos.flatMap(self.obter_dados_por_ano)
        # obter os dados da area colhida. Para obter os anos de 2018 ao atual, foi passado a função obter_dados_por_ano
        df_área_colhida = self.preprocess_data(spark.createDataFrame(dados_por_ano))

        # Obtendo dados dos estados a partir da API do IBGE
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        df_estado = spark.createDataFrame(requests.get(url).json()).select('sigla', col('nome').alias('nome_estado'))
        print("Imprimindo o schema de estado")
        df_estado.printSchema()

        # Usando a função obter_dados_por_ano em paralelo para obter os dados de todos os anos
        rdd_anos = spark.sparkContext.parallelize(anos)
        # Aplicando uma função lambda ao RDD para obter dados para cada ano
        # A função obter_dados_por_ano é chamada para cada ano com o número "214" como argumento
        dados_por_ano_1 = rdd_anos.flatMap(lambda ano: self.obter_dados_por_ano(ano, "214"))
        # Para obter os anos de 2018 ao atual, foi passado a função obter_dados_por_ano
        df_área_quantidade_produzida = self.preprocess_data(spark.createDataFrame(dados_por_ano_1))
        print("Imprimindo o schema da tabela do ibge")
        df_área_quantidade_produzida.printSchema()

        # cosolidar dados
        # ja que o area quantidade produzida e colhida tem os mesmos campos
        # nada mais justo de unir elas
        df_área_colhida_rel = df_área_colhida.union(df_área_quantidade_produzida)
        # cruzamento para com o estado do ibge foi com o intuito de obter o nome do estado completo
        # por exemplo PB, ao cruzar, fica Paraiba
        # e em seguida é realizada o drop de algumas colunas nao relevantes
        return df_área_colhida_rel.join(broadcast(df_estado), df_estado.sigla == df_área_colhida_rel.Estado,
                                        'inner').drop('Estado', 'sigla')

    def preprocess_data(self, df_área):
        return (
            df_área.withColumnRenamed('NC', 'Nível Territorial_cod_q')
            .withColumnRenamed('NN', 'Nível_Territorial_q')
            .withColumnRenamed('MC', 'Unidade_Medida_cod_q')
            .withColumnRenamed('MN', 'Unidade_Medida_q')
            .withColumnRenamed('V', 'Valor_q')
            .withColumnRenamed('D1C', 'Município_cod_q')
            .withColumnRenamed('D1N', 'Município_q')
            .withColumnRenamed('D2C', 'Variável_cod_q')
            .withColumnRenamed('D2N', 'Variável_q')
            .withColumnRenamed('D3C', 'Ano_cod_q')
            .withColumnRenamed('D3N', 'Ano_q')
            .withColumnRenamed('D4C', 'Produto_lavouras_temporárias_permanentes_cod_q')
            .withColumnRenamed('D4N', 'Produto_lavouras_temporárias_permanentes_q')
            .withColumn('Ano_q', col('Ano_q').cast('int'))
            .withColumn('Ano_cod_q', col('Ano_cod_q').cast('int'))
            .withColumn('Valor_q', col('Valor_q'))
            .withColumn('Município_cod_q', col('Município_cod_q').cast('int'))
            .withColumn('Variável_cod_q', col('Variável_cod_q').cast('int'))
            .withColumn('Produto_lavouras_temporárias_permanentes_cod_q',
                        col('Produto_lavouras_temporárias_permanentes_cod_q').cast('int'))
            .withColumn("Cidade", split(col("Município_q"), " - ")[0])
            .withColumn("Estado", split(col("Município_q"), " - ")[1])

        ).cache()