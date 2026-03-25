from transform import *
from db.connection import get_connection

# ============================================================
#  TABLE MAP — DataFrame → PostgreSQL Table Name
# ============================================================

# Maps dataset name → exact table name in your PostgreSQL schema
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
            name      = table_name,   # table name in PostgreSQL
            con       = engine,       # PostgreSQL engine
            if_exists = "append",     # append to existing table (schema already created)
            index     = False         # don't write DataFrame index as a column
        )
        print(f"  {name:<15}  Loaded → '{table_name}' ({len(df)} rows)")

    print("\n Load complete!")
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