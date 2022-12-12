from typing import List
from typing import Union

from pandas import DataFrame
from pandas import Timestamp
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.engine import URL


def clean_column_name(col_name: Union[str, List[str]]) -> Union[str, List[str]]:
    def _clean(name: str):
        return name.replace(" ", "_").replace("-", "_").lower()

    if isinstance(col_name, list):
        return [_clean(name) for name in col_name]
    elif isinstance(col_name, str):
        return _clean(col_name)
    else:
        raise ValueError("`col_name` should be of type `str` or `List[str]`")


def get_connection(
    drivername: str, 
    host: str, 
    username: str, 
    password: str, 
    database: str, 
    port: int = 5432
) -> Connection:
    conn_url = URL.create(
        drivername,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    )
    engine = create_engine(conn_url)
    conn = engine.connect()

    return conn


def write_to_db(
    conn: Connection, 
    df: DataFrame, 
    table: str, 
    schema: str, 
    include_index: bool = False, 
    table_strategy: str = "append"
) -> None:
    df["date_loaded"] = Timestamp.now()
    
    df.to_sql(
        table, 
        con=conn, 
        schema=schema, 
        index=include_index,
        if_exists=table_strategy
    )
