from .AbstractTable import * 
from ..constant import * 

from sqlalchemy.orm import Session 
from typing import Any, Optional, Iterator

__all__ = [
    'FlaskSQLAlchemyTable', 
]


def convert_sql(sql: str,
                params: Optional[list[Any]] = None) -> tuple[str, Optional[dict[str, Any]]]:
    param_cnt = 0 
    
    while True:
        index = sql.find('%s') 
        
        if index >= 0:
            sql = f"{sql[:index]}:param{param_cnt}{sql[index+2:]}"
            param_cnt += 1 
        else:
            break 
        
    if params is None:
        assert param_cnt == 0 

        return sql, None 
    else:
        assert len(params) == param_cnt 
    
        param_dict = {
            f"param{i}": param 
            for i, param in enumerate(params)
        }
        
        return sql, param_dict 


class FlaskSQLAlchemyTable(AbstractTable):
    def __init__(self,
                 session: Session, 
                 database: str, 
                 table: str):
        super().__init__()
        
        self.session = session 
        self.table = f"{database}.{table}"

    def execute_sql(self,
                    sql: str,
                    sql_type: int, 
                    params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
        sql, param_dict = convert_sql(sql=sql, params=params)
                    
        result = self.session.execute(
            statement = sql, 
            params = param_dict,
        )
        
        if sql_type == SELECT:
            entry_list = list(result.mappings())  
            return entry_list 
        else:
            self.session.commit() 
            return [] 
