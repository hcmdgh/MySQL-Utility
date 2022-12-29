from pymysql.connections import Connection 
from pymysql.cursors import Cursor 
from typing import Any, Optional, Iterator

__all__ = [
    'MySQLTable', 
]


class MySQLTable:
    def __init__(self,
                 conn: Connection,
                 cursor: Cursor,
                 database: str, 
                 table: str):
        self.conn = conn 
        self.cursor = cursor 
        self.table = f"{database}.{table}"
        
    def truncate_table(self):
        self.cursor.execute(f"TRUNCATE TABLE {self.table}")
        
    def insert_one(self,
                   entry: dict[str, Any]):
        self.cursor.execute(
            query = f"INSERT INTO {self.table} ({', '.join(entry.keys())}) VALUES ({', '.join(['%s'] * len(entry))})",
            args = list(entry.values()), 
        )
        
    def scan_table(self,
                   batch_size: int = 5000) -> Iterator[dict[str, Any]]:
        last_id = None 
            
        while True:
            if last_id is None:
                self.cursor.execute(
                    query = f"SELECT * FROM {self.table} ORDER BY id ASC LIMIT %s",
                    args = [batch_size],
                )
            else:
                self.cursor.execute(
                    query = f"SELECT * FROM {self.table} WHERE id > %s ORDER BY id ASC LIMIT %s",
                    args = [last_id, batch_size],
                )
            
            last_id = None 
            
            for entry in self.cursor.fetchall():
                last_id = entry['id']
                yield entry 

            if last_id is None:
                break 
            
    def query_X_eq_x(self,
                     X: str,
                     x: Any) -> list[dict[str, Any]]:
        self.cursor.execute(
            query = f"SELECT * FROM {self.table} WHERE {X} = %s", 
            args = [x], 
        )
        
        entry_list = list(self.cursor.fetchall())  
        
        return entry_list
    
    def query_X_eq_x_and_Y_eq_y(self,
                                X: str,
                                x: Any,
                                Y: str, 
                                y: Any) -> list[dict[str, Any]]:
        self.cursor.execute(
            query = f"SELECT * FROM {self.table} WHERE {X} = %s AND {Y} = %s", 
            args = [x, y], 
        )
        
        entry_list = list(self.cursor.fetchall())  
        
        return entry_list

    def query_by_id(self,
                    id: Any) -> Optional[dict[str, Any]]:
        self.cursor.execute(
            query = f"SELECT * FROM {self.table} WHERE id = %s", 
            args = [id],
        )
        
        entry_list = list(self.cursor.fetchall())  
        
        if not entry_list:
            return None 
        elif len(entry_list) == 1:
            return entry_list[0]
        else:
            raise AssertionError 
        
    def delete_by_id(self,
                     id: Any):
        self.cursor.execute(
            query = f"DELETE FROM {self.table} WHERE id = %s",
            args = [id], 
        )
