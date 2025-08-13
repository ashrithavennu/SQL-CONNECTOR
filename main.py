from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict
from fastapi import Body, Query

from db import (
    get_db_connection,
    create_runtime_table,
    get_connector_config,
    insert_runtime_data,
    validate_db_connection,
    create_json_table,
    insert_json_data,
    create_connector_table,
    insert_connector_config,
)

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class JSONPayload(BaseModel):
    host: str
    port: str
    schema: str
    data: List[Dict[str, object]]

class ConfigPayload(BaseModel):
    host: str
    port: str
    schema: str
    table_name: str
    app_id: int
    mapping: Dict[str, str]


@app.get("/")
async def root():
    return {"message": "SQL Connector is live."}

@app.post("/upload_json/")
async def upload_json(payload: JSONPayload):
    if not validate_db_connection(payload.host, payload.port, payload.schema):
        raise HTTPException(status_code=400, detail="Invalid DB credentials / schema")

    import pandas as pd
    try:
        df = pd.DataFrame(payload.data)
        if df.empty:
            raise ValueError("No records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    
    table_name = "json_invoice"
    headers = list(df.columns)
    rows = df.to_dict(orient="records")

    try:
        create_json_table(
            table_name=table_name,
            headers=headers,
            host=payload.host,
            port=payload.port,
            schema=payload.schema
          )

       
        mapping = {key: key for key in headers}

        insert_json_data(
            host=payload.host,
            port=payload.port,
            schema=payload.schema,
            table_name=table_name,
            mapping=mapping,
            data=rows
        )

        

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process JSON: {e}")

    return {
        "message": f"JSON uploaded and stored in table '{table_name}'",
        "headers": headers,
        "row_count": len(rows),
        "mapping": mapping,
    }

@app.post("/save_connector_config/")
async def save_config(cfg: ConfigPayload):
    create_connector_table(cfg.host, cfg.port, cfg.schema)

    config = {
        "host": cfg.host,
        "port": cfg.port,
        "schema": cfg.schema,
        "table_name": cfg.table_name,
        "app_id": cfg.app_id,
        "mapping": cfg.mapping
    }

    success = insert_connector_config(config, cfg.host, cfg.port, cfg.schema)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to insert connector config")

    return {"message": "Connector configuration saved successfully."}



@app.post("/runtime_process/")
async def runtime_process(app_id: int = Query(...), payload: dict = Body(...)):
    try:
        if "data" not in payload:
            raise HTTPException(status_code=400, detail="Missing 'data' in body.")
        
        data_list = payload["data"]
        if not isinstance(data_list, list):
            raise HTTPException(status_code=400, detail="'data' must be a list.")

        # Fetch config using app_id
        config = get_connector_config(app_id, host="localhost",port="5432",schema="docunotes")
        if not config:
            raise HTTPException(status_code=404, detail=f"No config found for app_id {app_id}")

        mapping = config.get("mapping")
        host = config.get("host")
        port = config.get("port")
        schema = config.get("schema")

        if not (mapping and host and port and schema):
            raise HTTPException(status_code=400, detail="Incomplete config: host, port, schema or mapping missing.")

        
        transformed_data = []
        for record in data_list:
            transformed = {json_key: record.get(csv_col, None) for json_key, csv_col in mapping.items()}
            transformed_data.append(transformed)

        table_name = f"runtime_app_{app_id}"
        create_runtime_table(table_name, transformed_data[0], host, port, schema)
        insert_runtime_data(table_name, transformed_data, host, port, schema)

        return {
            "message": f"Runtime data stored in table '{table_name}'",
            "rows": len(transformed_data),
            "table": table_name
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
