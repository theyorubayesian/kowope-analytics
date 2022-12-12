import argparse
import os
import traceback

import pandas as pd
from dotenv import load_dotenv

from helpers import *

load_dotenv()

DIM_CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "segment",
    "country",
    "city",
    "state",
    "postal_code",
    "region"
]

DIM_PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "sub_category",
    "category"
]

FACT_ORDERS_COLUMNS = [
    "order_id",
    "order_date",
    "ship_date",
    "customer_id",
    "product_id",
    "quantity",
    "discount",
    "profit",
    "sales"
]


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="EasySolar: CSV Uploader", 
        description="Upload CSV dataset to database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="Written by: Akintunde 'theyorubayesian' Oladipo <akin.o.oladipo@gmail.com>"
    )
    parser.add_argument("--csv-file", help="Path to [CSV file] to be uploaded")
    parser.add_argument("--customer-table", default="dim_customer", help="Database table for Customer data")
    parser.add_argument("--product-table", default="dim_product", help="Database table for Product data")
    parser.add_argument("--order-table", default="fact_order", help="Database table for Order data")
    parser.add_argument("--database", default="easysolar", help="Database for data")
    parser.add_argument("--schema", default="interview2", help="Database schema")
    parser.add_argument(
        "--table-strategy", 
        default="append", 
        choices=["fail", "replace", "append"], 
        help="Action to take if table exists"
    )
    parser.add_argument("--include-index", action="store_true", help="Include [CSV File] index in table")
    parser.add_argument("--db-drivername", default="postgresql", help="SQL Dialect to pass to SQLAlchemy")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    DB_HOST = os.getenv("EASYSOLAR_POSTGRES_HOST")
    DB_USER = os.getenv("EASYSOLAR_POSTGRES_USER")
    DB_PASSWORD = os.getenv("EASYSOLAR_POSTGRES_PASSWORD")
    DB_PORT = os.getenv("EASYSOLAR_POSTGRES_PORT")

    if DB_PORT is None:
        DB_PORT = 5432

    if any([DB_HOST is None, DB_USER is None, DB_PASSWORD is None]):
        raise ValueError(
            "Ensure that the following environment variables are set:"
            "`EASYSOLAR_POSTGRES_HOST`, `EASYSOLAR_POSTGRES_PORT`, "
            "`EASYSOLAR_POSTGRES_USER` and `EASYSOLAR_POSTGRES_PASSWORD`"
        )
    
    data = pd.read_csv(args.csv_file)
    data.columns = clean_column_name(list(data.columns))

    dim_customer_data = data[DIM_CUSTOMER_COLUMNS].copy()
    dim_product_data = data[DIM_PRODUCT_COLUMNS].copy()
    fact_orders_data = data[FACT_ORDERS_COLUMNS].copy()

    dfs = [dim_customer_data, dim_product_data, fact_orders_data]
    tables = [args.customer_table, args.product_table, args.order_table]

    db_conn = get_connection(
        args.db_drivername, 
        host=DB_HOST, 
        username=DB_USER, 
        password=DB_PASSWORD, 
        database=args.database,
        port=DB_PORT
    )

    for df, table in zip(dfs, tables):
        try:
            write_to_db(
                db_conn, 
                df=df, 
                table=table, 
                schema=args.schema, 
                include_index=args.include_index, 
                table_strategy=args.table_strategy
            )
        except Exception as e:
            print(f'Error writing dataframe to table: {table}')
            print(traceback.print_exc())
            continue


if __name__ == "__main__":
    main()
