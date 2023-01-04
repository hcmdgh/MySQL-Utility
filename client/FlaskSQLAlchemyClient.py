from ..table import * 

from flask import Flask 
from sqlalchemy.orm import Session 
from flask_sqlalchemy import SQLAlchemy

__all__ = [
    'FlaskSQLAlchemyClient', 
]


class FlaskSQLAlchemyClient:
    def __init__(self,
                 *, 
                 app: Flask,
                 host: str,
                 port: int = 3306,
                 user: str, 
                 password: str):
        self.db = SQLAlchemy()
        app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{user}:{password}@{host}:{port}"
        self.db.init_app(app)            
    
    def get_table(self,
                  database: str,
                  table: str) -> PyMySQLTable:
        return FlaskSQLAlchemyTable(
            session = self.db.session, 
            database = database, 
            table = table, 
        ) 
