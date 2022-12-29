from .table import * 

import pymysql
import pymysql.cursors

__all__ = [
    'MySQLClient', 
]


class MySQLClient:
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
        
    def close_connection(self):
        self.conn.close()
    
    def get_table(self,
                  database: str,
                  table: str) -> MySQLTable:
        return MySQLTable(
            conn = self.conn, 
            cursor = self.cursor, 
            database = database, 
            table = table, 
        ) 
