import pandas as pd
import sqlalchemy as sa

# ============================================================
#  CONFIGURATION — Base Path
# ============================================================

BASE_PATH = r"C:\Users\ACER\Downloads\hospital_etl_datasources"

# ============================================================
#  EXTRACT — Load CSV Files
# ============================================================

def load_data():
    df_appoint   = pd.read_csv(f"{BASE_PATH}\\appointments_etl.csv")
    df_bill      = pd.read_csv(f"{BASE_PATH}\\billing_etl.csv")
    df_doctors   = pd.read_csv(f"{BASE_PATH}\\doctors_etl.csv")
    df_patient   = pd.read_csv(f"{BASE_PATH}\\patients_etl.csv")
    df_treatment = pd.read_csv(f"{BASE_PATH}\\treatments_etl.csv")
    return df_appoint, df_bill, df_doctors, df_patient, df_treatment

# ============================================================
#  PREVIEW — First 5 Rows of Each Table
# ============================================================

def preview(datasets):
    print("\n--- Preview ---")
    for name, df in datasets.items():
        print(f"\n--- {name} ---")
        print(df.head())

# ============================================================
#  SHAPE — Rows x Columns of Each Table
# ============================================================

def shape(datasets):
    print("\n--- Shape ---")
    for name, df in datasets.items():
        print(f"  {name:<15} {df.shape}")

# ============================================================
#  COLUMNS & DTYPES — Side by Side
# ============================================================

def columns_dtypes(datasets):
    print("\n--- Columns & Dtypes ---")
    for name, df in datasets.items():
        print(f"\n--- {name} ---")
        print(f"  {'Column':<30} {'Dtype'}")
        for col, dtype in df.dtypes.items():
            print(f"  {col:<30} {dtype}")

# ============================================================
#  MAIN — Run All Steps
# ============================================================

def main():
    df_appoint, df_bill, df_doctors, df_patient, df_treatment = load_data()

    datasets = {
        "Appointments" : df_appoint,
        "Billing"      : df_bill,
        "Doctors"      : df_doctors,
        "Patients"     : df_patient,
        "Treatments"   : df_treatment,
    }

    preview(datasets)
    shape(datasets)
    columns_dtypes(datasets)

    print("\n Extraction complete!")
    return df_appoint, df_bill, df_doctors, df_patient, df_treatment

df_appoint, df_bill, df_doctors, df_patient, df_treatment = main()