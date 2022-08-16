from flask import Flask
from flaskext.mysql import MySQL
from marshmallow import Schema, fields
from flask import jsonify, request
import pymysql


#Contantes
SQL_CLIENTE_POR_ID = 'SELECT id, nome, cpf, telefone, email FROM cliente WHERE id = %s'
SQL_LISTA_END = 'SELECT id, rua, numero, complemento, bairro, cep, cidade, uf, cliente_id FROM endereco'
SQL_END_POR_ID = 'SELECT id, rua, numero, complemento, bairro, cep, cidade, uf, cliente_id FROM endereco WHERE id = %s'
SQL_END_CLIENTE_ID = 'SELECT id, rua, numero, complemento, bairro, cep, cidade, uf FROM endereco WHERE cliente_id = %s'
SQL_CRIA_END = 'INSERT INTO endereco (rua, numero, complemento, cep, bairro, cidade, uf, cliente_id) values (%s, %s, %s, %s, %s, %s, %s, %s)'
SQL_DELETA_END = 'DELETE FROM endereco WHERE id = %s'
SQL_ATUALIZA_END = 'UPDATE endereco SET rua = %s, numero = %s, complemento = %s, cep = %s, bairro = %s, cidade = %s, uf = %s, cliente_id = %s WHERE id = %s'
SQL_USUARIO_SENHA = 'SELECT id FROM usuario WHERE nome = %s AND senha = %s'


class ClienteSchema(Schema):
    id = fields.Integer()
    nome = fields.String()
    email = fields.String()
    cpf = fields.String()
    telefone = fields.String()


class EnderecoSchema(Schema):
    id = fields.Integer()
    bairro = fields.String()
    cep = fields.String()
    cidade = fields.String()
    complemento = fields.String()
    numero = fields.Integer()
    rua = fields.String()
    uf = fields.String()
    cliente = fields.Nested(ClienteSchema())


#Configuracao Banco de Dados
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

#auth_services

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


def buscar_enderecos():
    db = DbHelper()
    enderecos = db.query_all(SQL_LISTA_END)
    for endereco in enderecos:
        endereco["cliente"] = db.query_one_filter(SQL_CLIENTE_POR_ID, endereco["cliente_id"])
    db.disconnect()
    return enderecos


def buscar_endereco_id(id):
    db = DbHelper()
    endereco = db.query_one_filter(SQL_END_POR_ID, id)
    endereco["cliente"] = db.query_one_filter(SQL_CLIENTE_POR_ID, endereco["cliente_id"])
    db.disconnect()
    return endereco


def adicionar_endereco(body):
    dados = validar_novo_endereco(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_CRIA_END, dados)
        db.disconnect()
        resp = {"error": False, "message": "Endereço criado com sucesso!"}
        return resp
    else:
        return None


def validar_novo_endereco(dados):
    _rua = dados["rua"]
    _numero = dados["numero"]
    _complemento = dados["complemento"]
    _cep = dados["cep"]
    _bairro = dados["bairro"]
    _cidade = dados["cidade"]
    _uf = dados["uf"]
    _cliente_id = dados["cliente_id"]
    if _rua and _numero and _complemento and _cep and _bairro and _cidade and _uf and _cliente_id:
        return _rua, _numero, _complemento, _cep, _bairro, _cidade, _uf, _cliente_id
    else:
        return None


def atualizar_endereco(body):
    dados = validar_endereco_atualizado(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_ATUALIZA_END, dados)
        db.disconnect()
        resp = {"error": False, "message": "Endereco atualizado com sucesso!"}
        return resp
    else:
        return None


def validar_endereco_atualizado(dados):
    _id = dados['id']
    info = validar_novo_endereco(dados)
    if info and _id:
        lista = list(info)
        lista.append(_id)
        return tuple(lista)
    else:
        return None


def remover_endereco(id):
    db = DbHelper()
    db.execute(SQL_DELETA_END, id)
    db.disconnect()
    return {"error": False, "message": "Endereço excluído com sucesso!"}



@app.route('/endereco', methods=['GET'])
#@auth.login_required
def listar_endereco():
    try:
        resp = jsonify(EnderecoSchema().dump(buscar_enderecos(), many=True))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/endereco/<int:id>', methods=['GET'])
#@auth.login_required
def listar_endereco_id(id):
    try:
        resp = jsonify(EnderecoSchema().dump(buscar_endereco_id(id)))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/endereco', methods=['POST'])
#@auth.login_required
def criar_endereco():
    try:
        info = adicionar_endereco(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/endereco', methods=['PUT'])
#@auth.login_required
def alterar_endereco():
    try:
        info = atualizar_endereco(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/endereco/<int:id>', methods=['DELETE'])
#@auth.login_required
def deletar_endereco(id):
    try:
        resp = jsonify(remover_endereco(id))
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
    app.run(port=5002)
