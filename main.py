from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncpg
from typing import Dict, List, Any

app = FastAPI()


class DbParams(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str


class TableRequest(BaseModel):
    table: str


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],  # Angular app's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variable to hold the connection pool
pool = None


async def get_conn():
    global pool
    async with pool.acquire() as conn:
        yield conn


async def fetch_metadata(conn):
    tables_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    """
    relationships_query = """
    SELECT
        tc.table_name AS table_name,
        ccu.table_name AS foreign_table_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
    WHERE constraint_type = 'FOREIGN KEY'
    """

    tables = await conn.fetch(tables_query)
    relationships = await conn.fetch(relationships_query)

    elements = []

    table_nodes = {table['table_name']: f"table_{table['table_name']}" for table in tables}

    for table in tables:
        elements.append(
            {"data": {"id": table_nodes[table['table_name']], "label": table['table_name'], "type": "table"}}
        )

    for rel in relationships:
        source_id = table_nodes[rel['table_name']]
        target_id = table_nodes.get(rel['foreign_table_name'], rel['foreign_table_name'])
        elements.append(
            {"data": {"id": f"edge_{source_id}_to_{target_id}", "source": source_id, "target": target_id,
                      "type": "relationship"}}
        )

    return {"elements": elements}


async def fetch_columns(table: str, conn) -> Dict[str, List[Dict[str, Any]]]:
    columns_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = '{table}'
    """

    columns = await conn.fetch(columns_query)
    column_elements = []
    edge_elements = []

    table_id = f"table_{table}"
    table_node = {"data": {"id": table_id, "label": table, "type": "table"}}

    for column in columns:
        column_id = f"{table}_{column['column_name']}"
        column_elements.append(
            {"data": {"id": column_id, "label": column['column_name'], "type": "column"}}
        )
        edge_elements.append(
            {"data": {"id": f"edge_{table}_to_{column_id}", "source": table_id, "target": column_id,
                      "type": "column-relationship"}}
        )

    return {"elements": column_elements + edge_elements}


@app.post("/api/db-connect")
async def db_connect(params: DbParams):
    global pool
    try:
        # Close any existing pool
        if pool:
            await pool.close()

        # Create a new connection pool with the provided parameters
        pool = await asyncpg.create_pool(
            user=params.username,
            password=params.password,
            database=params.database,
            host=params.host,
            port=params.port
        )

        # Acquire a connection to fetch metadata
        async with pool.acquire() as conn:
            metadata = await fetch_metadata(conn)

        return metadata
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/columns")
async def get_columns(request: TableRequest, conn=Depends(get_conn)):
    try:
        columns = await fetch_columns(request.table, conn)
        return columns
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
