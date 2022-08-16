from flask import Flask
#from flask_httpauth import HTTPBasicAuth
from flaskext.mysql import MySQL
from flask import jsonify, request
import pymysql
from marshmallow import Schema, fields

SQL_CLIENTE_POR_ID = 'SELECT id, nome, cpf, telefone, email FROM cliente WHERE id = %s'
SQL_END_POR_ID = 'SELECT id, rua, numero, complemento, bairro, cep, cidade, uf FROM endereco WHERE id = %s'
SQL_LISTA_PED_CLIENTE_ID = 'SELECT id, data_compra, cliente_id, endereco_id FROM pedido WHERE cliente_id = %s'
SQL_LISTA_PED_END_ID = 'SELECT id, data_compra, cliente_id, endereco_id FROM pedido WHERE endereco_id = %s'
SQL_LISTA_PED = 'SELECT id, data_compra, cliente_id, endereco_id FROM pedido'
SQL_LISTA_PED_ID = 'SELECT id, data_compra, cliente_id, endereco_id FROM pedido WHERE id = %s'
SQL_LISTA_PROD_PED_ID = 'SELECT p.id, p.velocidade, p.preco, p.descricao, p.disponibilidade, i.qtde FROM produto p INNER JOIN itens_pedido i ON p.id = i.produto_id WHERE i.pedido_id = %s'
SQL_MAX_PED_ID = 'SELECT MAX(id) FROM pedido'
SQL_CRIA_PED = 'INSERT INTO pedido (data_compra, cliente_id, endereco_id) values (%s, %s, %s)'
SQL_CRIA_ITENS_PED = 'INSERT INTO itens_pedido (qtde, pedido_id, produto_id) values (%s, %s, %s)'
SQL_ATUALIZA_ITENS_PED = "UPDATE itens_pedido SET qtde=%s WHERE pedido_id = %s and produto_id = %s"
SQL_DELETA_ITENS_PED = 'DELETE FROM itens_pedido WHERE pedido_id = %s'
SQL_DELETA_PED = 'DELETE FROM pedido WHERE id = %s'
SQL_DELETA_ITENS_PED_PROD = 'DELETE FROM itens_pedido WHERE pedido_id = %s and produto_id = %s'
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


class ProdutoSchema(Schema):
    id = fields.Integer()
    velocidade = fields.String()
    preco = fields.Decimal()
    descricao = fields.String()
    qtde = fields.Integer()


class PedidoSchema(Schema):
    id = fields.Integer()
    data_compra = fields.Date()
    cliente = fields.Nested(ClienteSchema())
    endereco = fields.Nested(EnderecoSchema())
    produto = fields.List(fields.Nested(ProdutoSchema()))


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

    def query_all_filter(self, str_quey, id):  # executa uma consulta  e retorna todos os dados filtrados
        self.__cursor.execute(str_quey, id)
        return self.__cursor.fetchall()

    def query_one(self, str_quey):  # executa uma consulta e retorna 1 dado
        self.__cursor.execute(str_quey)
        return self.__cursor.fetchone()

    def query_one_filter(self, str_quey, args):  # executa uma consulta  e retorna 1 dado filtrado
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


def buscar_pedidos():
    db = DbHelper()
    cliente_id = request.args.get("clienteId")
    endereco_id = request.args.get("enderecoId")
    pedidos = buscar_pedidos_bd(db, cliente_id, endereco_id)
    for pedido in pedidos:
        pedido["cliente"] = db.query_one_filter(SQL_CLIENTE_POR_ID, pedido["cliente_id"])
        pedido["endereco"] = db.query_one_filter(SQL_END_POR_ID, pedido["endereco_id"])
        pedido["produto"] = db.query_all_filter(SQL_LISTA_PROD_PED_ID, pedido["id"])
    db.disconnect()
    return pedidos


def buscar_pedidos_bd(db, cliente_id, endereco_id):
    if cliente_id is not None:
        return db.query_all_filter(SQL_LISTA_PED_CLIENTE_ID, cliente_id)
    elif endereco_id is not None:
        return db.query_all_filter(SQL_LISTA_PED_END_ID, endereco_id)
    else:
        return db.query_all(SQL_LISTA_PED)


def buscar_pedido_id(id):
    db = DbHelper()
    pedido = db.query_one_filter(SQL_LISTA_PED_ID, id)
    if pedido:
        pedido["cliente"] = db.query_one_filter(SQL_CLIENTE_POR_ID, pedido["cliente_id"])
        pedido["endereco"] = db.query_one_filter(SQL_END_POR_ID, pedido["endereco_id"])
        pedido["produto"] = db.query_all_filter(SQL_LISTA_PROD_PED_ID, pedido["id"])
    db.disconnect()
    return pedido


def adicionar_pedido(body):
    dados = validar_novo_pedido(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_CRIA_PED, (dados[0], dados[1], dados[2]))
        pedido_id = db.query_one(SQL_MAX_PED_ID)
        for produto in dados[3]:
            db.execute(SQL_CRIA_ITENS_PED, (produto["qtde"], pedido_id["MAX(id)"], produto["produto_id"]))
        db.disconnect()
        resp = {"error": False, "message": "Pedido criado com sucesso!"}
        return resp
    else:
        return None


def validar_novo_pedido(dados):
    _data_compra = dados["data_compra"]
    _cliente_id = dados["cliente_id"]
    _endereco_id = dados["endereco_id"]
    _produto = dados["produto"]
    if _data_compra and _cliente_id and _endereco_id and _produto:
        return _data_compra, _cliente_id, _endereco_id, _produto
    else:
        return None


def adicionar_itens_pedido(body):
    dados = validar_novo_item(body)
    if dados is not None:
        db = DbHelper()
        for produto in dados[1]:
            db.execute(SQL_CRIA_ITENS_PED, (produto["qtde"], dados[0], produto["produto_id"]))
        db.disconnect()
        resp = {"error": False, "message": "Produtos adicionados com sucesso!"}
        return resp
    else:
        return None


def validar_novo_item(dados):
    _pedido_id = dados["pedido_id"]
    _novos_itens = dados["novos_produtos"]
    if _pedido_id and _novos_itens:
        return _pedido_id, _novos_itens
    else:
        return None


def atualizar_pedido(body):
    dados = validar_dados_alteracao(body)
    if dados is not None:
        db = DbHelper()
        db.execute(SQL_ATUALIZA_ITENS_PED, (dados[0], dados[1], dados[2]))
        db.disconnect()
        resp = {"error": False, "message": "Pedido atualizado com sucesso!"}
        return resp
    else:
        return None


def validar_dados_alteracao(dados):
    _pedido_id = dados["pedido_id"]
    _produto_id = dados["produto_id"]
    _qtde = dados["nova_qtde"]
    if _pedido_id and _produto_id and _qtde:
        return _qtde, _pedido_id, _produto_id
    else:
        return None


def remover_pedido():
    db = DbHelper()
    pedido_id = request.args.get("pedidoid")
    produto_id = request.args.get("produtoid")
    msg = excluir_pedido_bd(db, pedido_id, produto_id)
    db.disconnect()
    return msg


def excluir_pedido_bd(db, pedido, produto):
    if pedido is None and produto is None:
        return {"error": True, "message": "Para excluir informe id pedido / id produto"}
    elif pedido is None and produto is not None:
        return {"error": True, "message": "Para excluir o produto informe id pedido"}
    elif pedido is not None and produto is None:
        db.execute(SQL_DELETA_ITENS_PED, pedido)
        db.execute(SQL_DELETA_PED, pedido)
        return {"error": False, "message": "Pedido excluido com sucesso!"}
    else:
        db.execute(SQL_DELETA_ITENS_PED_PROD, (pedido, produto))
        return {"error": False, "message": "Produto excluido do pedido com sucesso!"}


@app.route('/pedidos', methods=['GET'])
#@auth.login_required
def listar_pedidos():
    try:
        resp = jsonify(PedidoSchema().dump(buscar_pedidos(), many=True))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/pedido/<int:id>', methods=['GET'])
#@auth.login_required
def listar_pedido_id(id):
    try:
        resp = jsonify(PedidoSchema().dump(buscar_pedido_id(id)))
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)


@app.route('/pedidos', methods=['POST'])
#@auth.login_required
def criar_pedido():
    try:
        info = adicionar_pedido(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/pedido/produto', methods=['POST'])
#@auth.login_required
def adicionar_produto():
    try:
        info = adicionar_itens_pedido(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/pedidos', methods=['PUT'])
#@auth.login_required
def alterar_pedido():
    try:
        info = atualizar_pedido(request.json)
        if info is not None:
            resp = jsonify(info)
            resp.status_code = 200
            return resp
        else:
            return show_message()
    except Exception as e:
        print(e)


@app.route('/pedido', methods=['DELETE'])
#@auth.login_required
def deletar_pedido():
    try:
        resp = jsonify(remover_pedido())
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
    app.run(port=5004)