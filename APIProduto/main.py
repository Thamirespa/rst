from flask import Flask
from flaskext.mysql import MySQL
from flask import jsonify, request
import pymysql


SQL_LISTA_PROD = 'SELECT id, velocidade, preco, descricao, disponibilidade FROM produto'
SQL_LISTA_PROD_ID = 'SELECT id, velocidade, preco, descricao, disponibilidade FROM produto WHERE id = %s'
SQL_LISTA_PROD_PED_ID = 'SELECT p.id, p.velocidade, p.preco, p.descricao, p.disponibilidade, i.qtde FROM produto p INNER JOIN itens_pedido i ON p.id = i.produto_id WHERE i.pedido_id = %s'
SQL_CRIA_PROD = 'INSERT INTO produto (velocidade, preco, descricao, disponibilidade) values (%s, %s, %s, %s)'
SQL_DELETA_PROD = 'DELETE FROM produto WHERE id = %s'
SQL_ATUALIZA_PROD = "UPDATE produto SET velocidade=%s, preco=%s, descricao=%s, disponibilidade=%s WHERE id=%s"
SQL_USUARIO_SENHA = 'SELECT id FROM usuario WHERE nome = %s AND senha = %s'


class DbMysql():
    def __init__(self, app):
        self.mysql = MySQL()
        self.config(app)
        self.mysql.init_app(app)
        self.msg()

    def config(self, app):
        app.config['MYSQL_DATABASE_USER'] = 'admin'
        app.config['MYSQL_DATABASE_PASSWORD'] = 'admin123'
        app.config['MYSQL_DATABASE_DB'] = 'db_cliente'
        app.config['MYSQL_DATABASE_HOST'] = 'database-1.cxycaymkd24m.us-east-1.rds.amazonaws.com'

    def msg(self):
        print("banco de dados db_api_flask conectado!")

    def get_mysql(self):
        return self.mysql

#Db Helper
app = Flask(__name__)
#auth = HTTPBasicAuth()
db = DbMysql(app).get_mysql()

class DbHelper:
    def __init__(self):  # construtor, cria os atributos da classe
        self.__connect()

    def __connect(self):  # função conectar com o banco e cria cursor
        self.__connection = db.connect()
        self.__cursor = self.__connection.cursor(pymysql.cursors.DictCursor)

    def disconnect(self):  # função desconectar do banco e limpa os atributos
        self.__cursor.close()
        self.__connection.close()

    def query_all(self, str_quey):  # executa uma consulta  e retorna todos os dados
        self.__cursor.execute(str_quey)
        return self.__cursor.fetchall()

    def query_all_filter(self, str_quey, id):  # executa uma consulta  e retorna todos os dados
        self.__cursor.execute(str_quey, id)
        return self.__cursor.fetchall()

    def query_one_filter(self, str_quey, args):  # executa uma consulta  e retorna todos os dados
        self.__cursor.execute(str_quey, args)
        return self.__cursor.fetchone()

    def execute(self, str_command, args):
        self.__cursor.execute(str_command, args)
        self.__connection.commit()


#@auth.verify_password
def authenticate(username, password):
    db = DbHelper()
    id_usuario = db.query_one_filter(SQL_USUARIO_SENHA, (username, password))
    if id_usuario is not None:
        return True
    return False


#@auth.error_handler
def auth_error(status):
    message = {
        'error': True,
        'message': 'Acesso não autorizado!',
    }
    return message, status


def buscar_produtos():
    db = DbHelper()
    produtos = db.query_all(SQL_LISTA_PROD)
    db.disconnect()
    return produtos


def buscar_produto_id(id):
    db = DbHelper()
    produto = db.query_one_filter(SQL_LISTA_PROD_ID, id)
    db.disconnect()
    return produto


def adicionar_produto(body):
    dados = validar_novo_produto(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_CRIA_PROD, dados)
        db.disconnect()
        resp = {"error": False, "message": "Produto criado com sucesso!"}
        return resp
    else:
        return None


def validar_novo_produto(dados):
    _velocidade = dados['velocidade']
    _preco = dados['preco']
    _descricao = dados['descricao']
    _disponibilidade = dados['disponibilidade']
    if _velocidade and _preco and _descricao and _disponibilidade:
        return _velocidade, _preco, _descricao, _disponibilidade
    else:
        return None


def atualizar_produto(body):
    dados = validar_produto_atualizado(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_ATUALIZA_PROD, dados)
        db.disconnect()
        resp = {"error": False, "message": "Produto atualizado com sucesso!"}
        return resp
    else:
        return None


def validar_produto_atualizado(dados):
    _id = dados['id']
    info = validar_novo_produto(dados)
    if info and _id:
        lista = list(info)
        lista.append(_id)
        return tuple(lista)
    else:
        return None


def remover_produto(id):
    db = DbHelper()
    db.execute(SQL_DELETA_PROD, id)
    db.disconnect()
    return {"error": False, "message": "Produto excluído com sucesso!"}


@app.route('/produtos', methods=['GET'])
#@auth.login_required
def listar_produto():
    try:
        resp = jsonify(buscar_produtos())
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/produto/<int:id>', methods=['GET'])
#@auth.login_required
def listar_produto_id(id):
    try:
        resp = jsonify(buscar_produto_id(id))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/produtos', methods=['POST'])
#@auth.login_required
def criar_produto():
    try:
        info = adicionar_produto(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/produtos', methods=['PUT'])
#@auth.login_required
def alterar_produto():
    try:
        info = atualizar_produto(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/produto/<int:id>', methods=['DELETE'])
#@auth.login_required
def deletar_produto(id):
    try:
        resp = jsonify(remover_produto(id))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.errorhandler(404)
def show_message(error=None):
    message = {
        'error': True,
        'message': error if error is not None else 'Not found',
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


if __name__ == "__main__":
    app.run(port=5003)