from extractor import *

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
#  TRANSFORM — Fill Missing Values
# ============================================================

def fill_appointments(df):
    df['doctor_id']        = df['doctor_id'].fillna('Unknown')
    df['appointment_time'] = df['appointment_time'].fillna('00:00')
    df['reason_for_visit'] = df['reason_for_visit'].fillna('Not Specified')
    df['status']           = df['status'].fillna('Unknown')
    return df


# --- Billing ---
def fill_billing(df):
    df['amount']         = pd.to_numeric(df['amount'], errors='coerce')  # str → float first
    df['amount']         = df['amount'].fillna(df['amount'].median())
    df['payment_method'] = df['payment_method'].fillna('Unknown')
    df['payment_status'] = df['payment_status'].fillna('Pending')
    return df


def fill_doctors(df):
    df['phone_number']     = df['phone_number'].fillna('Not Available')
    df['years_experience'] = df['years_experience'].fillna(df['years_experience'].median())
    df['email']            = df['email'].fillna('Not Available')
    return df

def fill_patients(df):
    df['gender']             = df['gender'].fillna('Unknown')
    df['date_of_birth']      = df['date_of_birth'].fillna('1900-01-01')
    df['contact_number']     = df['contact_number'].fillna('Not Available')
    df['address']            = df['address'].fillna('Not Provided')
    df['insurance_provider'] = df['insurance_provider'].fillna('None')
    df['insurance_number']   = df['insurance_number'].fillna('None')
    df['email']              = df['email'].fillna('Not Available')
    return df

# --- Treatments ---
def fill_treatments(df):
    df['cost']           = pd.to_numeric(df['cost'], errors='coerce')    # str → float first
    df['cost']           = df['cost'].fillna(df['cost'].median())
    df['treatment_type'] = df['treatment_type'].fillna('General')
    df['description']    = df['description'].fillna('No Description')
    return df


# ============================================================
#  TRANSFORM — Fix Data Types
# ============================================================

def fix_dtypes(datasets):
    # --- Appointments ---
    df = datasets["Appointments"]
    df['appointment_date'] = pd.to_datetime(df['appointment_date'], errors='coerce')
    df['appointment_time'] = pd.to_datetime(df['appointment_time'], format='%H:%M:%S', errors='coerce').dt.time

    # --- Billing ---
    df = datasets["Billing"]
    df['bill_date'] = pd.to_datetime(df['bill_date'], errors='coerce')
    df['amount']    = pd.to_numeric(df['amount'], errors='coerce')

    # --- Doctors ---
    df = datasets["Doctors"]
    df['phone_number']     = pd.to_numeric(df['phone_number'], errors='coerce')
    df['years_experience'] = pd.to_numeric(df['years_experience'], errors='coerce')

    # --- Patients ---
    df = datasets["Patients"]
    df['date_of_birth']      = pd.to_datetime(df['date_of_birth'], errors='coerce')
    df['registration_date']  = pd.to_datetime(df['registration_date'], errors='coerce')
    df['contact_number']     = pd.to_numeric(df['contact_number'], errors='coerce')

    # --- Treatments ---
    df = datasets["Treatments"]
    df['cost']           = pd.to_numeric(df['cost'], errors='coerce')
    df['treatment_date'] = pd.to_datetime(df['treatment_date'], errors='coerce')




# ============================================================
#  NULL CHECK — After Transform
# ============================================================

def null_check_after(datasets):
    print("\n--- Null Check After Transform ---")
    for name, df in datasets.items():
        total_nulls = df.isnull().sum().sum()
        status = "✅ Clean" if total_nulls == 0 else f"⚠️  {total_nulls} nulls remain"
        print(f"  {name:<15} {status}")  # left pad to 15 chars so lines up cleanly

# ============================================================
#  MAIN — Run All Steps
# ============================================================

def main():
    null_check_before(datasets)

    df_appoint   = fill_appointments(datasets["Appointments"])
    df_bill      = fill_billing(datasets["Billing"])
    df_doctors   = fill_doctors(datasets["Doctors"])
    df_patient   = fill_patients(datasets["Patients"])
    df_treatment = fill_treatments(datasets["Treatments"])

    datasets.update({
        "Appointments" : df_appoint,
        "Billing"      : df_bill,
        "Doctors"      : df_doctors,
        "Patients"     : df_patient,
        "Treatments"   : df_treatment,
    })

    fix_dtypes(datasets)       # ← add this line
    null_check_after(datasets)
    print("\n✅ Transform complete!")