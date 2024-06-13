
from flask import Flask, request, jsonify
from services import AreaColhidaService

app = Flask(__name__)
class AreaColhidaController:



    @app.route('/area_colhida', methods=['GET'])
    def get_area_colhida():
        repo = AreaColhidaService()
        municipio = request.args.get('municipio')
        ano = request.args.get('ano')
        print(municipio,ano)
        area_colhida = repo.get_area_colhida(municipio, ano)

        response = {
            'success': False,
            'data': None,
            'message': ''
        }
        if area_colhida:
            response['success'] = True
            response['data'] = {'area_colhida': area_colhida}
        else:
            response['message'] = 'Dados não encontrados'

        return jsonify(response)


    @app.route('/produtividade', methods=['GET'])
    def get_produtividade():
        repo = AreaColhidaService()
        ano = request.args.get('ano')
        estados = request.args.getlist('estados')
        response = {
            'success': False,
            'data': None,
            'message': ''
        }
        try:
            resultado = repo.get_produtividade(ano, estados)
            response['success'] = True
            response['data'] = resultado
        except Exception as e:
            response['message'] = str(e)

        return jsonify(response)



if __name__ == '__main__':
    app.run()