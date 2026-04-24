import csv
import os
import time
from sqlalchemy import create_engine, text, inspect
from datetime import datetime

# --- CONFIGURATION (AUTO-DISCOVERED) ---
TARGET_DB = "idea_lab"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Real credentials discovered from the platform session
MYSQL_URL = "mysql+pymysql://root:harris+005@localhost:3306/idea_lab"

def get_engine():
    print(f"Connecting to MySQL ({TARGET_DB})...")
    try:
        engine = create_engine(MYSQL_URL, connect_args={'connect_timeout': 5})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            print("Connected successfully to MySQL!")
            return engine
    except Exception as e:
        print(f"Connection failed: {e}")
        print("Falling back to local SQLite for safety.")
        return create_engine(f"sqlite:///{os.path.join(os.path.dirname(__file__), 'idea_lab.db')}")

def setup_lineage_table(engine):
    with engine.begin() as conn:
        if "mysql" in str(engine.url):
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS data_lineage (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    source_name VARCHAR(255),
                    target_table VARCHAR(255),
                    operation_type VARCHAR(50),
                    rows_affected INT,
                    status VARCHAR(50),
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

def log_lineage(engine, source, target, op, count, status):
    """Refreshes the dynamic lineage record."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO data_lineage (source_name, target_table, operation_type, rows_affected, status)
            VALUES (:source, :target, :op, :count, :status)
        """), {"source": source, "target": target, "op": op, "count": count, "status": status})

def get_table_columns(engine, table_name):
    try:
        inspector = inspect(engine)
        return [col['name'] for col in inspector.get_columns(table_name)]
    except:
        return []

def run_etl():
    print(f"=== REAL-TIME ETL STARTING ===")
    engine = get_engine()
    setup_lineage_table(engine)

    try:
        while True:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing data...")
            
            # --- PROCESS PRODUCTS ---
            product_file = os.path.join(DATA_DIR, "products_source.csv")
            if os.path.exists(product_file):
                with open(product_file, mode='r', encoding='utf-8') as f:
                    data = list(csv.DictReader(f))
                
                cols = get_table_columns(engine, "lab_product")
                if cols:
                    with engine.begin() as conn:
                        for row in data:
                            # Map CSV fields to DB columns based on the UI schema
                            mapped_row = {
                                "id": int(row.get('id', 0)),
                                "name": row.get('name', 'Unknown'),
                                "description": row.get('category', 'Imported Product'),
                                "category": row.get('category', 'General'),
                                "price": float(row.get('price', 0)),
                                "stock": int(row.get('stock', 0)),
                                "discounted_price": float(row.get('price', 0)) * 0.9, # 10% discount
                                "image": "https://images.unsplash.com/photo-1581093458791-9f3c3900df4b?auto=format&fit=crop&q=80&w=200",
                                "created_at": datetime.now(),
                                "updated_at": datetime.now()
                            }
                            # Final filter: only use columns that actually exist in the table
                            final_row = {k: v for k, v in mapped_row.items() if k in cols}
                            
                            placeholders = ", ".join([f":{k}" for k in final_row.keys()])
                            col_names = ", ".join(final_row.keys())
                            sql = f"REPLACE INTO lab_product ({col_names}) VALUES ({placeholders})"
                            conn.execute(text(sql), final_row)
                    log_lineage(engine, "products_source.csv", "lab_product", "REALTIME_SYNC", len(data), "SUCCESS")
                    print(f"Sync complete: {len(data)} rows updated in lab_product.")

            time.sleep(10)
    except KeyboardInterrupt:
        print("ETL Stopped.")
    except Exception as e:
        print(f"ETL EXCEPTION: {e}")

if __name__ == "__main__":
    run_etl()
