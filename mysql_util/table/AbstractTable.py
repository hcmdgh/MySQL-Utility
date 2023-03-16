from ..constant import * 

from typing import Any, Optional, Iterator

__all__ = [
    'AbstractTable', 
]


class AbstractTable:
    def __init__(self):
        self.table = ''
    
    def execute_sql(self,
                    sql: str,
                    sql_type: int, 
                    params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
        raise NotImplementedError
    
    def count(self) -> int:
        result = self.execute_sql(
            sql = f"SELECT COUNT(*) FROM {self.table}", 
            sql_type = SELECT, 
        )
        
        assert len(result) == 1 and len(result[0]) == 1 
        cnt = list(result[0].values())[0]
        assert isinstance(cnt, int)
        
        return cnt 
        
    def truncate_table(self):
        self.execute_sql(
            sql = f"TRUNCATE TABLE {self.table}",
            sql_type = TRUNCATE, 
        )
        
    def insert_one(self,
                   entry: dict[str, Any]):
        self.execute_sql(
            sql = f"INSERT INTO {self.table} ({', '.join(entry.keys())}) VALUES ({', '.join(['%s'] * len(entry))})",
            sql_type = INSERT, 
            params = list(entry.values()), 
        )
        
    def query_X_eq_x(self,
                     X: str,
                     x: Any) -> list[dict[str, Any]]:
        return self.execute_sql(
            sql = f"SELECT * FROM {self.table} WHERE {X} = %s", 
            sql_type = SELECT, 
            params = [x], 
        )
        
    def query_X_in_x(self,
                     X: str,
                     x: Any) -> list[dict[str, Any]]:
        return self.execute_sql(
            sql = f"SELECT * FROM {self.table} WHERE {X} IN %s", 
            sql_type = SELECT, 
            params = [list(x)], 
        )
    
    def query_X_eq_x_and_Y_eq_y(self,
                                X: str,
                                x: Any,
                                Y: str, 
                                y: Any) -> list[dict[str, Any]]:
        return self.execute_sql(
            sql = f"SELECT * FROM {self.table} WHERE {X} = %s AND {Y} = %s", 
            sql_type = SELECT, 
            params = [x, y], 
        )
        
    def query_X_in_x_and_Y_in_y(self,
                                X: str,
                                x: Any,
                                Y: str, 
                                y: Any) -> list[dict[str, Any]]:
        return self.execute_sql(
            sql = f"SELECT * FROM {self.table} WHERE {X} IN %s AND {Y} IN %s", 
            sql_type = SELECT, 
            params = [list(x), list(y)], 
        )

    def query_by_id(self,
                    id: Any) -> Optional[dict[str, Any]]:
        entry_list = self.execute_sql(
            sql = f"SELECT * FROM {self.table} WHERE id = %s", 
            sql_type = SELECT, 
            params = [id],
        )
        
        if not entry_list:
            return None 
        elif len(entry_list) == 1:
            return entry_list[0]
        else:
            raise AssertionError 
        
    def delete_by_id(self,
                     id: Any):
        self.execute_sql(
            sql = f"DELETE FROM {self.table} WHERE id = %s",
            sql_type = DELETE, 
            params = [id], 
        )

    def update_by_id(self,
                     id: Any,
                     **key_value):
        assert key_value 
        
        self.execute_sql(
            sql = f"UPDATE {self.table} SET {', '.join(f'{key} = %s' for key in key_value.keys())} WHERE id = %s",
            sql_type = UPDATE, 
            params = list(key_value.values()) + [id], 
        )

    def drop_table(self):
        self.execute_sql(
            sql = f"DROP TABLE IF EXISTS {self.table}", 
            sql_type = DROP, 
        )
