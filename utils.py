import sqlite3
from marshmallow import ValidationError

DB_PATH = 'dados_nordeste.db'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def validate_data(schema, data):
    try:
        validated = schema.load(data)
        return validated, None
    except ValidationError as err:
        return None, err.messages
