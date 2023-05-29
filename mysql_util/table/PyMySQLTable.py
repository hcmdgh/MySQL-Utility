from .AbstractTable import * 
from ..constant import * 

from pymysql.connections import Connection 
from pymysql.cursors import Cursor 
from typing import Any, Optional, Iterator

__all__ = [
    'PyMySQLTable', 
]


class PyMySQLTable(AbstractTable):
    def __init__(self,
                 conn: Connection,
                 cursor: Cursor,
                 database: str, 
                 table: str):
        super().__init__()
        
        self.conn = conn 
        self.cursor = cursor 
        self.table = f"`{database}`.`{table}`"

    def execute_sql(self,
                    sql: str,
                    sql_type: int, 
                    params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
        self.cursor.execute(
            query = sql, 
            args = params,
        )
        
        if sql_type == SELECT:
            entry_list = list(self.cursor.fetchall())  
            return entry_list 
        else:
            return []
        
    def scan_table(self,
                   batch_size: int = 5000,
                   pk_name: str = 'id') -> Iterator[dict[str, Any]]:
        last_id = None 
            
        while True:
            if last_id is None:
                self.cursor.execute(
                    query = f"SELECT * FROM {self.table} ORDER BY {pk_name} ASC LIMIT %s",
                    args = [batch_size],
                )
            else:
                self.cursor.execute(
                    query = f"SELECT * FROM {self.table} WHERE {pk_name} > %s ORDER BY {pk_name} ASC LIMIT %s",
                    args = [last_id, batch_size],
                )
            
            last_id = None 
            
            for entry in self.cursor.fetchall():
                last_id = entry[pk_name]
                yield entry 

            if last_id is None:
                break 
            