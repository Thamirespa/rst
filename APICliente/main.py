from flask import Flask
#from flask_httpauth import HTTPBasicAuth
from flaskext.mysql import MySQL
from flask import jsonify, request
import pymysql
from marshmallow import Schema, fields

SQL_LISTA_CLIENTE = 'SELECT id, nome, cpf, telefone, email FROM cliente'
SQL_CLIENTE_POR_ID = 'SELECT id, nome, cpf, telefone, email FROM cliente WHERE id = %s'
SQL_CRIA_CLIENTE = 'INSERT INTO cliente (nome, cpf, telefone, email) values (%s, %s, %s, %s)'
SQL_DELETA_CLIENTE = 'DELETE FROM cliente WHERE id = %s'
SQL_ATUALIZA_CLIENTE = "UPDATE cliente SET nome=%s, cpf=%s, telefone=%s, email=%s WHERE id=%s"
SQL_END_CLIENTE_ID = 'SELECT id, rua, numero, complemento, bairro, cep, cidade, uf FROM endereco WHERE cliente_id = %s'
SQL_USUARIO_SENHA = 'SELECT id FROM usuario WHERE nome = %s AND senha = %s'

class EnderecoSchema(Schema):
    id = fields.Integer()
    bairro = fields.String()
    cep = fields.String()
    cidade = fields.String()
    complemento = fields.String()
    numero = fields.Integer()
    rua = fields.String()
    uf = fields.String()


class ClienteEnderecoSchema(Schema):
    id = fields.Integer()
    nome = fields.String()
    email = fields.String()
    cpf = fields.String()
    telefone = fields.String()
    endereco = fields.List(fields.Nested(EnderecoSchema()))

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


def buscar_clientes():
    db = DbHelper()
    clientes = db.query_all(SQL_LISTA_CLIENTE)
    db.disconnect()
    return clientes


def buscar_cliente_endereco():
    clientes = buscar_clientes()
    db = DbHelper()
    for cliente in clientes:
        cliente["endereco"] = db.query_all_filter(SQL_END_CLIENTE_ID, cliente["id"])
    db.disconnect()
    return clientes


def buscar_cliente_id(id):
    db = DbHelper()
    cliente = db.query_one_filter(SQL_CLIENTE_POR_ID, id)
    db.disconnect()
    return cliente


def adicionar_cliente(body):
    dados = validar_novo_cliente(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_CRIA_CLIENTE, dados)
        db.disconnect()
        resp = {"error": False, "message": "Cliente criado com sucesso!"}
        return resp
    else:
        return None


def validar_novo_cliente(dados):
    _nome = dados['nome']
    _cpf = dados['cpf']
    _telefone = dados['telefone']
    _email = dados['email']
    if _nome and _cpf and _email and _telefone:
        return _nome, _cpf, _telefone, _email
    else:
        return None


def atualizar_cliente(body):
    dados = validar_cliente_atualizado(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_ATUALIZA_CLIENTE, dados)
        db.disconnect()
        resp = {"error": False, "message": "Cliente atualizado com sucesso!"}
        return resp
    else:
        return None


def validar_cliente_atualizado(dados):
    _id = dados['id']
    info = validar_novo_cliente(dados)
    if info and _id:
        lista = list(info)
        lista.append(_id)
        return tuple(lista)
    else:
        return None


def remover_cliente(id):
    db = DbHelper()
    db.execute(SQL_DELETA_CLIENTE, id)
    db.disconnect()
    return {"error": False, "message": "Cliente excluído com sucesso!"}



@app.route('/clientes', methods=['GET'])
#@auth.login_required
def listar_cliente():
    try:
        resp = jsonify(buscar_clientes())
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/cliente_endereco', methods=['GET'])
#@auth.login_required
def listar_cliente_endereco():
    try:
        resp = jsonify(ClienteEnderecoSchema().dump(buscar_cliente_endereco(), many=True))
        return resp
    except Exception as e:
        print(e)


@app.route('/cliente/<int:id>', methods=['GET'])
#@auth.login_required
def listar_cliente_id(id):
    try:
        resp = jsonify(buscar_cliente_id(id))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/clientes', methods=['POST'])
#@auth.login_required
def criar_cliente():
    try:
        info = adicionar_cliente(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/clientes', methods=['PUT'])
#@auth.login_required
def alterar_cliente():
    try:
        info = atualizar_cliente(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/cliente/<int:id>', methods=['DELETE'])
#@auth.login_required
def deletar_cliente(id):
    try:
        resp = jsonify(remover_cliente(id))
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
    app.run(port=5001)
