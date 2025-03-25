from sqlalchemy import text

def create_table_if_not_exists(engine, table_name: str):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        bid_open REAL,
        bid_high REAL,
        bid_low REAL,
        bid_close REAL,
        ask_open REAL,
        ask_high REAL,
        ask_low REAL,
        ask_close REAL,
        bid_volume REAL,
        ask_volume REAL,
        avg_spread REAL
    );
    SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);
    """
    with engine.connect() as conn:
        for stmt in create_sql.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
        conn.commit()
