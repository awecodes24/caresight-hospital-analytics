import pandas as pd
from extractor import df_appoint, df_bill, df_doctors, df_patient, df_treatment
from logging_monitoring.logger import  setup_logger

sns.set(style="whitegrid")

# ============================================================
#  DATASETS — Dictionary of All Tables
# ============================================================

datasets = {
    "Appointments" : df_appoint,
    "Billing"      : df_bill,
    "Doctors"      : df_doctors,
    "Patients"     : df_patient,
    "Treatments"   : df_treatment,
}

# ============================================================
#  NULL CHECK — Before Transform
# ============================================================

def null_check_before(datasets):
    print("\n--- Null Check Before Transform ---")
    for name, df in datasets.items():
        print(f"\n{name} :\n", df.isnull().sum())

# ============================================================
#  TRANSFORM — Fix Data Types FIRST
# ============================================================

def fix_dtypes(datasets):
    df = datasets["Appointments"]
    df['appointment_date'] = pd.to_datetime(df['appointment_date'], errors='coerce')
    df['appointment_time'] = pd.to_datetime(
        df['appointment_time'].astype(str).str.strip(),
        format='mixed', errors='coerce'
    ).dt.time

    df = datasets["Billing"]
    df['bill_date'] = pd.to_datetime(df['bill_date'], errors='coerce')
    df['amount']    = pd.to_numeric(df['amount'], errors='coerce')

    df = datasets["Doctors"]
    df['phone_number']     = pd.to_numeric(df['phone_number'], errors='coerce')
    df['years_experience'] = pd.to_numeric(df['years_experience'], errors='coerce')

    df = datasets["Patients"]
    df['date_of_birth']     = pd.to_datetime(df['date_of_birth'], errors='coerce')
    df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
    df['contact_number']    = pd.to_numeric(df['contact_number'], errors='coerce')

    df = datasets["Treatments"]
    df['cost']           = pd.to_numeric(df['cost'], errors='coerce')
    df['treatment_date'] = pd.to_datetime(df['treatment_date'], errors='coerce')

# ============================================================
#  TRANSFORM — Fill Missing Values AFTER dtype fix
# ============================================================

def fill_appointments(df):
    df['doctor_id']        = df['doctor_id'].fillna('Unknown')
    df['appointment_time'] = df['appointment_time'].fillna(pd.Timestamp('00:00:00').time())
    df['reason_for_visit'] = df['reason_for_visit'].fillna('Not Specified')
    df['status']           = df['status'].fillna('Unknown')
    return df

def fill_billing(df):
    median_amount        = df['amount'].median(skipna=True)
    df['amount']         = df['amount'].fillna(median_amount if pd.notna(median_amount) else 0)
    df['payment_method'] = df['payment_method'].fillna('Unknown')
    df['payment_status'] = df['payment_status'].fillna('Pending')
    return df

def fill_doctors(df):
    df['phone_number']     = df['phone_number'].fillna(0)
    df['years_experience'] = df['years_experience'].fillna(df['years_experience'].median())
    df['email']            = df['email'].fillna('Not Available')
    return df

def fill_patients(df):
    df['gender']             = df['gender'].fillna('Unknown')
    df['date_of_birth']      = df['date_of_birth'].fillna(pd.Timestamp('1900-01-01'))
    df['contact_number']     = df['contact_number'].fillna(0)
    df['address']            = df['address'].fillna('Not Provided')
    df['registration_date']  = df['registration_date'].fillna(pd.Timestamp('1900-01-01'))
    df['insurance_provider'] = df['insurance_provider'].fillna('None')
    df['insurance_number']   = df['insurance_number'].fillna('None')
    df['email']              = df['email'].fillna('Not Available')
    return df

def fill_treatments(df):
    df['cost']           = df['cost'].fillna(df['cost'].median())
    df['treatment_type'] = df['treatment_type'].fillna('General')
    df['description']    = df['description'].fillna('No Description')
    df['treatment_date'] = df['treatment_date'].fillna(pd.Timestamp('1900-01-01'))
    return df

# ============================================================
#  NULL CHECK — After Transform
# ============================================================

def drop_remaining_nulls(datasets):
    for name in ["Appointments", "Billing"]:
        before = len(datasets[name])
        datasets[name] = datasets[name].dropna()
        after = len(datasets[name])
        setup_logger.warning(f"{name}: Dropped {before - after} rows")

    setup_logger.info("Transformation complete!")
    for name, df in datasets.items():
        total_nulls = df.isnull().sum().sum()
        status = "✅ Clean" if total_nulls == 0 else f"⚠️  {total_nulls} nulls remain"
        print(f"  {name:<15} {status}")


# ============================================================
#  MAIN — Run All Steps
# ============================================================

def main():
    null_check_before(datasets)

    fix_dtypes(datasets)

    datasets["Appointments"] = fill_appointments(datasets["Appointments"])
    datasets["Billing"]      = fill_billing(datasets["Billing"])
    datasets["Doctors"]      = fill_doctors(datasets["Doctors"])
    datasets["Patients"]     = fill_patients(datasets["Patients"])
    datasets["Treatments"]   = fill_treatments(datasets["Treatments"])

    print("\n--- Dropping Remaining Nulls ---")
    drop_remaining_nulls(datasets)

    # null_check_after(datasets)
    print("\n Transform complete!")

    # ← EDA runs right after null check
    run_eda(datasets)

main()