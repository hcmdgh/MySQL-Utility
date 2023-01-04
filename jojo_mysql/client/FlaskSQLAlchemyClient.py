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
                 host: str,
                 port: int = 3306,
                 user: str, 
                 password: str):
        self.host = host 
        self.port = port 
        self.user = user 
        self.password = password
                 
        self.db = None 
    
    def init_app(self,
                 app: Flask):
        self.db = SQLAlchemy()

        app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}"
        app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False 

        self.db.init_app(app)   
        
    def get_table(self,
                  database: str,
                  table: str) -> FlaskSQLAlchemyTable:
        assert self.db is not None 
                  
        return FlaskSQLAlchemyTable(
            session = self.db.session, 
            database = database, 
            table = table, 
        ) 
