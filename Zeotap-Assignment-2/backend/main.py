from fastapi import FastAPI, HTTPException, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from clickhouse_driver import Client
import pandas as pd
import jwt
from typing import List
import logging

# FastAPI app initialization
app = FastAPI()


logging.basicConfig(level=logging.DEBUG)

@app.post("/connect_clickhouse")
def connect_clickhouse(host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        logging.debug(f"Connecting to ClickHouse with host={host}, port={port}, database={database}, user={user}")
        client = Client(host=host, port=port, database=database, user=user, password=jwt_token)
        tables = client.execute("SHOW TABLES")
        logging.debug(f"Tables in database: {tables}")
        return {"status": "success", "tables": [t[0] for t in tables]}
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
# Enable CORS for all origins (for frontend interaction)
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Secret Key for JWT
SECRET_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X3VzZXIiLCJpYXQiOjE2MDgxNTk3MzUsImV4cCI6MTYwODE2MzMzNX0.-OobUOgdBfgmM44ewbyz3hXMwTwtTHh09VskzmFgx1o"
# ClickHouse Connection
@app.post("/connect_clickhouse")
def connect_clickhouse(host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        # Validate JWT Token
        decoded_token = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        
        # Establishing connection to ClickHouse
        client = Client(host=host, port=port, database=database, user=user, password=decoded_token['password'])
        tables = client.execute("SHOW TABLES")
        return {"status": "success", "tables": [t[0] for t in tables]}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=400, detail="JWT token expired")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Get columns of a table
@app.post("/get_columns")
def get_columns(table: str = Form(...), host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        decoded_token = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        client = Client(host=host, port=port, database=database, user=user, password=decoded_token['password'])
        columns = client.execute(f"DESCRIBE TABLE {table}")
        return {"columns": [col[0] for col in columns]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Ingest data from ClickHouse to Flat File (CSV)
@app.post("/ingest_clickhouse_to_file")
def ingest_clickhouse_to_file(table: str = Form(...), columns: str = Form(...), host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        column_list = columns.split(',')
        decoded_token = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        client = Client(host=host, port=port, database=database, user=user, password=decoded_token['password'])
        query = f"SELECT {', '.join(column_list)} FROM {table}"
        data = client.execute(query)
        df = pd.DataFrame(data, columns=column_list)
        file_path = f"{table}_export.csv"
        df.to_csv(file_path, index=False)
        return {"status": "success", "records": len(df), "file": file_path}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Ingest data from Flat File to ClickHouse
@app.post("/ingest_file_to_clickhouse")
def ingest_file_to_clickhouse(file: UploadFile, table: str = Form(...), host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        logging.debug(f"Received file: {file.filename}, table: {table}, host: {host}, port: {port}, database: {database}")
        df = pd.read_csv(file.file)
        
        logging.debug(f"Dataframe shape: {df.shape}")
        
        decoded_token = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        logging.debug(f"Decoded JWT Token: {decoded_token}")
        
        client = Client(host=host, port=port, database=database, user=user, password=decoded_token['password'])
        columns = ', '.join(df.columns)
        
        for index, row in df.iterrows():
            values = ', '.join([f"'{str(v)}'" for v in row])
            query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
            logging.debug(f"Executing query: {query}")
            client.execute(query)
        
        return {"status": "success", "records": len(df)}
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/connect_clickhouse")
def connect_clickhouse(host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        logging.debug(f"Connecting to ClickHouse with host={host}, port={port}, database={database}, user={user}")
        client = Client(host=host, port=port, database=database, user=user, password=jwt_token)
        tables = client.execute("SHOW TABLES")
        logging.debug(f"Tables in database: {tables}")
        return {"status": "success", "tables": [t[0] for t in tables]}
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
@app.post("/get_join_columns")
def get_join_columns(tables: str = Form(...), host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        decoded_token = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        client = Client(host=host, port=port, database=database, user=user, password=decoded_token['password'])
        table_list = tables.split(',')
        column_names = []
        for table in table_list:
            result = client.execute(f"DESCRIBE TABLE {table.strip()}")
            column_names.extend([f"{table.strip()}.{col[0]}" for col in result])
        return {"columns": column_names}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/ingest_join_to_file")
def ingest_join_to_file(tables: str = Form(...), join_condition: str = Form(...), columns: str = Form(...), host: str = Form(...), port: int = Form(...), database: str = Form(...), user: str = Form(...), jwt_token: str = Form(...)):
    try:
        decoded_token = jwt.decode(jwt_token, SECRET_KEY, algorithms=["HS256"])
        client = Client(host=host, port=port, database=database, user=user, password=decoded_token['password'])

        table_list = tables.split(',')
        column_list = columns.split(',')

        from_clause = f"{table_list[0]} " + " ".join([f"JOIN {tbl} ON {join_condition}" for tbl in table_list[1:]])
        query = f"SELECT {', '.join(column_list)} FROM {from_clause}"

        data = client.execute(query)
        df = pd.DataFrame(data, columns=column_list)
        file_path = f"joined_export.csv"
        df.to_csv(file_path, index=False)
        return {"status": "success", "records": len(df), "file": file_path}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    