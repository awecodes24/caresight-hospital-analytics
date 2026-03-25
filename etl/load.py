import sys
import os

# ── Make project root importable (must be BEFORE any project imports) ──────────
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from transform import datasets          # datasets dict built at module level
from db.connection import get_connection

# ============================================================
#  TABLE MAP — DataFrame → PostgreSQL Table Name
# ============================================================

TABLE_MAP = {
    "Appointments" : "appointments",
    "Billing"      : "bills",
    "Doctors"      : "doctors",
    "Patients"     : "patients",
    "Treatments"   : "treatments",
}

# ============================================================
#  LOAD — Save DataFrames into PostgreSQL
# ============================================================

def load_data(datasets):
    engine = get_connection()

    print("\n--- Loading Tables into PostgreSQL ---")
    for name, df in datasets.items():
        table_name = TABLE_MAP[name]
        df.to_sql(
            name      = table_name,
            con       = engine,
            if_exists = "replace",
            index     = False
        )
        print(f"  {name:<15}  Loaded → '{table_name}' ({len(df)} rows)")

    print("\n✅ Load complete!")
    return engine

# ============================================================
#  VERIFY — Read Back from PostgreSQL
# ============================================================

def verify_load(engine):
    print("\n--- Verify Load (first 5 rows) ---")
    for name, table_name in TABLE_MAP.items():
        print(f"\n--- {name} ---")
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", con=engine)
        print(df)

# ============================================================
#  MAIN — Run All Steps
# ============================================================

def main():
    engine = load_data(datasets)
    verify_load(engine)

main()
