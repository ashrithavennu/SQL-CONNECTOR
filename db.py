import os, json, psycopg2, logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection(host, port, schema):
    try:
        logger.info(f"Connecting to DB with host={host}, port={port}, schema={schema}")
        conn = psycopg2.connect(
            dbname=schema,
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=host,
            port=port
        )
        return conn
    except Exception as e:
        logger.exception("Database connection failed")
        raise

def validate_db_connection(host, port, schema):
    try:
        conn = get_db_connection(host, port, schema)
        conn.close()
        return True
    except Exception as e:
        logger.error(f"DB validation failed: {e}")
        return False

def create_json_table(table_name, headers, host, port, schema):
    try:
        conn = get_db_connection(host, port, schema)
        cur = conn.cursor()
        cols = ", ".join([f'"{h}" TEXT' for h in headers])
        full_table_name = f'"{schema}"."{table_name}"'
        cur.execute(f'CREATE TABLE IF NOT EXISTS {full_table_name} (id SERIAL PRIMARY KEY, {cols})')
        conn.commit()
        logger.info(f"Ensured JSON table {full_table_name}")
    except Exception as e:
        logger.exception("Failed to create JSON table")
    finally:
        cur.close()
        conn.close()

def insert_json_data(host, port, schema, table_name, mapping, data):
    if not mapping or not data:
        logger.error("Missing mapping or data")
        return

    try:
        conn = get_db_connection(host, port, schema)
        cursor = conn.cursor()

        full_table = f'"{schema}"."{table_name}"'

        for record in data:
            columns = [f'"{mapping[key]}"' for key in mapping]
            values = [record.get(key) for key in mapping]
            placeholders = ", ".join(["%s"] * len(values))

            insert_query = f'''
                INSERT INTO {full_table} ({", ".join(columns)})
                VALUES ({placeholders})
            '''
            cursor.execute(insert_query, values)

        conn.commit()
        logger.info(f"Inserted {len(data)} records into {full_table}")
    except Exception as e:
        logger.exception("Error inserting JSON data")
    finally:
        cursor.close()
        conn.close()

def create_connector_table(host, port, schema):
    try:
        conn = get_db_connection(host, port, schema)
        cur = conn.cursor()
        cur.execute(f"""
          CREATE TABLE IF NOT EXISTS connector (
            connector_id SERIAL PRIMARY KEY,
            connector_type TEXT DEFAULT 'output',
            config JSONB,
            created_time TIMESTAMPTZ DEFAULT NOW()
          )
        """)
        conn.commit()
        logger.info("Ensured connector table exists.")
    except Exception as e:
        logger.exception("Failed to create connector table")
    finally:
        cur.close()
        conn.close()

def insert_connector_config(config, host, port, schema):
    conn = None
    try:
        conn = get_db_connection(host, port, schema)
        if conn is None:
            raise Exception("Database connection failed.")
        cur = conn.cursor()

        insert_query = """
        INSERT INTO connector (config)
        VALUES (%s)
        """
        cur.execute(insert_query, (json.dumps(config),))
        conn.commit()
        logger.info("Inserted connector config successfully.")
        return True
    except Exception as e:
        logger.exception("Failed to insert connector config")
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

def get_connector_config(app_id, host, port, schema="public"):
    try:
        conn = get_db_connection(host, port, schema)
        cur = conn.cursor()
        cur.execute("SELECT config FROM connector")
        rows = cur.fetchall()
        for row in rows:
            config = row[0]
            if str(config.get("app_id")) == str(app_id):
                logger.info(f"Found config for app_id={app_id}")
                return config
    except Exception as e:
        logger.exception("Error retrieving connector config")
    finally:
        cur.close()
        conn.close()
    return None

def create_runtime_table(table_name, sample_row, host, port, schema):
    try:
        conn = get_db_connection(host, port, schema)
        cur = conn.cursor()
        columns_sql = ", ".join([f'"{key}" TEXT' for key in sample_row])
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id SERIAL PRIMARY KEY,
                {columns_sql}
            )
        """)
        conn.commit()
        logger.info(f"Created runtime table '{table_name}'")
    except Exception as e:
        logger.exception("Failed to create runtime table")
    finally:
        cur.close()
        conn.close()

def insert_runtime_data(table_name, rows: list, host, port, schema):
    try:
        conn = get_db_connection(host, port, schema)
        cur = conn.cursor()
        for row in rows:
            columns = ', '.join([f'"{k}"' for k in row.keys()])
            placeholders = ', '.join(['%s'] * len(row))
            values = list(row.values())
            cur.execute(f"""
                INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})
            """, values)
        conn.commit()
        logger.info(f"Inserted {len(rows)} rows into runtime table '{table_name}'")
    except Exception as e:
        logger.exception("Failed to insert runtime data")
    finally:
        cur.close()
        conn.close()
