from ..table import * 

import pymysql
import pymysql.cursors
from typing import Optional 

__all__ = [
    'MySQLClient', 
    'PyMySQLClient', 
]


class PyMySQLClient:
    def __init__(self,
                 *, 
                 host: str,
                 port: int = 3306,
                 user: str, 
                 password: str):
        self.conn = pymysql.connect(
            host = host,
            port = port, 
            user = user,
            password = password,
            charset = 'utf8mb4',
            autocommit = True, 
            cursorclass = pymysql.cursors.DictCursor,
        )
        
        self.cursor = self.conn.cursor()
        
    def close(self):
        self.conn.close()
    
    def get_table(self,
                  table_1: str,
                  table_2: Optional[str] = None) -> PyMySQLTable:
        if not table_2:
            database, _, table = table_1.partition('.')
        else:
            database, table = table_1, table_2
        assert database and table 
                  
        return PyMySQLTable(
            conn = self.conn, 
            cursor = self.cursor, 
            database = database, 
            table = table, 
        ) 


MySQLClient = PyMySQLClient 
