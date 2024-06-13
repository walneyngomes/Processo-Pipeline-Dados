import sqlite3
from silver-camada import SilverData
class GoldData:

    def delete(self,year: int, **kwargs):
        try:
            query =f"DELETE FROM tabela_final_gold where Ano_q = {year}"

            for key, value in kwargs.items():
                if (value != ""):
                    query += f" AND {key}={value}"

            conn = sqlite3.connect('/home/wa/base_gold_ibge.db')
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
        except sqlite3.Error as e:
            print(f"Erro ao inserir ou atualizar dados: {e}")
        finally:
            conn.close()


    def insert_or_update(self,year: int, **kwargs):
        try:
            conn = sqlite3.connect('/home/wa/base_gold_ibge.db')
            cursor = conn.cursor()

            # Verificar se já existe um registro para o ano especificado
            cursor.execute("SELECT EXISTS (SELECT 1 FROM tabela_final_gold WHERE Ano_q = ?)", (year,))
            exists = cursor.fetchone()[0]

            if exists:
            # Atualizar os dados existentes
                update_query = f"UPDATE tabela_final_gold SET {', '.join(f'{key} = ?' for key in kwargs.keys())} WHERE Ano_q = ?"
                values = tuple(kwargs.values()) + (year,)
                cursor.execute(update_query, values)
                print(f"Dados atualizados para o ano {year}")
            else:
                # Inserir um novo registro
                columns = ', '.join(kwargs.keys())
                placeholders = ', '.join('?' * len(kwargs))
                insert_query = f"INSERT INTO tabela_final_gold (Ano_q, {columns}) VALUES (?, {placeholders})"
                values = (year,) + tuple(kwargs.values())
                cursor.execute(insert_query, values)
                print(f"Novo registro inserido para o ano {year}")

            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            print(f"Erro ao inserir ou atualizar dados: {e} , possivelmente voce nao tem permssão de gravar ou escrever dados")







if __name__ == "__main__":
    teclado = int(input(f" Deletar 1  \n Inserir or atualizar - 2 \n QUal acao voce deseja realizar"))
    silver_dados = SilverData()
    gold_dados = GoldData()

    if(teclado==1):
        gold_dados.delete(int(input('digite o ano')), Municipio_cod_q=(input("Digite o código do município (inteiro): "))
               , Municipio_q=input("Digite o nome do município: ")
               , Variavel_cod_q=(input("Digite o código da variável (inteiro): "))
               , Variavel_q=input("Digite a variável: ")
               , Ano_cod_q=(input("Digite o código do ano (inteiro): "))
               , Ano_q=(input("Digite o ano: "))
               , Produto_lavouras_temporarias_permanentes_cod_q=(
                input("Digite o código do produto de lavouras temporárias/permanentes (inteiro): "))
               , Produto_lavouras_temporarias_permanentes_q=input(
                "Digite o produto de lavouras temporárias/permanentes: ")
               , Unidade_Medida_cod_q=input("Digite o código da unidade de medida: ")
               , Unidade_Medida_q=input("Digite a unidade de medida: ")
               , Nivel_Territorial_cod_q=input("Digite o código do nível territorial: ")
               , Nivel_Territorial_q=input("Digite o nível territorial: ")
               , Valor_q=(input("Digite o valor (float): "))
               , Cidade=input("Digite a cidade: ")
               , nome_estado=input("Digite o nome do estado: ")
               )

    else:
        gold_dados.insert_or_update(int(input('digite o ano')),
                        Municipio_cod_q=(input("Digite o código do município (inteiro): "))
                         , Municipio_q=input("Digite o nome do município: ")
                         , Variavel_cod_q=(input("Digite o código da variável (inteiro): "))
                         , Variavel_q=input("Digite a variável: ")
                         , Ano_cod_q=(input("Digite o código do ano (inteiro): "))
                         , Ano_q=(input("Digite o ano: "))
                         , Produto_lavouras_temporarias_permanentes_cod_q=(
                input("Digite o código do produto de lavouras temporárias/permanentes (inteiro): "))
                         , Produto_lavouras_temporarias_permanentes_q=input(
                "Digite o produto de lavouras temporárias/permanentes: ")
                         , Unidade_Medida_cod_q=input("Digite o código da unidade de medida: ")
                         , Unidade_Medida_q=input("Digite a unidade de medida: ")
                         , Nivel_Territorial_cod_q=input("Digite o código do nível territorial: ")
                         , Nivel_Territorial_q=input("Digite o nível territorial: ")
                         , Valor_q=(input("Digite o valor (float): "))
                         , Cidade=input("Digite a cidade: ")
                         , nome_estado=input("Digite o nome do estado: ")
                         )

