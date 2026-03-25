import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from extractor import df_appoint, df_bill, df_doctors, df_patient, df_treatment

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
        logger.warning(f"{name}: Dropped {before - after} rows")

    logger.info("Transformation complete!")
    for name, df in datasets.items():
        total_nulls = df.isnull().sum().sum()
        status = "✅ Clean" if total_nulls == 0 else f"⚠️  {total_nulls} nulls remain"
        print(f"  {name:<15} {status}")

# ============================================================
#  EDA — Build Merged DataFrame for Analysis
# ============================================================

def build_analysis_df(datasets):
    """Merge all tables into one flat DataFrame for EDA."""
    df = datasets["Billing"].copy()
    df = df.merge(datasets["Appointments"][['appointment_id','patient_id','doctor_id','appointment_date','status','reason_for_visit']],
                  on=['patient_id'], how='left')
    df = df.merge(datasets["Doctors"][['doctor_id','first_name','last_name','specialization']].rename(
                      columns={'first_name':'doctor_first_name','last_name':'doctor_last_name'}),
                  on='doctor_id', how='left')
    df = df.merge(datasets["Patients"][['patient_id','first_name','last_name']].rename(
                      columns={'first_name':'patient_first_name','last_name':'patient_last_name'}),
                  on='patient_id', how='left')
    df = df.merge(datasets["Treatments"][['treatment_id','treatment_date','treatment_type']],
                  on='treatment_id', how='left')
    df['month'] = pd.to_datetime(df['bill_date']).dt.to_period('M')
    return df

# ============================================================
#  EDA — Line Charts
# ============================================================

def plot_revenue_trend(df):
    revenue_trend = df.groupby('month')['amount'].sum().reset_index()
    plt.figure(figsize=(10, 5))
    plt.plot(revenue_trend['month'].astype(str), revenue_trend['amount'], marker='o')
    plt.xticks(rotation=45)
    plt.title('Hospital Revenue Trend Over Time')
    plt.xlabel('Month')
    plt.ylabel('Revenue')
    plt.tight_layout()
    plt.show()

def plot_appointments_per_month(df):
    appointments_per_month = df.groupby(
        df['appointment_date'].dt.to_period('M'))['appointment_id'].nunique().reset_index()
    appointments_per_month.rename(columns={'appointment_id': 'num_appointments'}, inplace=True)
    plt.figure(figsize=(10, 5))
    plt.plot(appointments_per_month['appointment_date'].astype(str),
             appointments_per_month['num_appointments'], marker='o')
    plt.xticks(rotation=45)
    plt.title('Number of Appointments Per Month')
    plt.xlabel('Month')
    plt.ylabel('Number of Appointments')
    plt.tight_layout()
    plt.show()

def plot_treatment_volume(df):
    treatment_volume_month = df.groupby(
        df['treatment_date'].dt.to_period('M'))['treatment_id'].nunique().reset_index()
    treatment_volume_month.rename(columns={'treatment_id': 'num_treatments'}, inplace=True)
    plt.figure(figsize=(10, 5))
    plt.plot(treatment_volume_month['treatment_date'].astype(str),
             treatment_volume_month['num_treatments'], marker='o', color='orange')
    plt.xticks(rotation=45)
    plt.title('Treatment Volume Over Time (Monthly)')
    plt.xlabel('Month')
    plt.ylabel('Number of Treatments')
    plt.tight_layout()
    plt.show()

# ============================================================
#  EDA — Bar Charts
# ============================================================

def plot_top_doctors_by_revenue(df):
    doctor_revenue = df.groupby(['doctor_first_name', 'doctor_last_name'])['amount'].sum().reset_index()
    doctor_revenue['doctor_name'] = doctor_revenue['doctor_first_name'] + " " + doctor_revenue['doctor_last_name']
    doctor_revenue = doctor_revenue.sort_values('amount', ascending=False).head(10)
    plt.figure(figsize=(10, 5))
    sns.barplot(x='amount', y='doctor_name', data=doctor_revenue)
    plt.title('Top 10 Doctors by Revenue')
    plt.xlabel('Revenue')
    plt.ylabel('Doctor')
    plt.tight_layout()
    plt.show()

def plot_top_patients_by_ltv(df):
    patient_revenue = df.groupby(['patient_first_name', 'patient_last_name'])['amount'].sum().reset_index()
    patient_revenue['patient_name'] = patient_revenue['patient_first_name'] + " " + patient_revenue['patient_last_name']
    patient_revenue = patient_revenue.sort_values('amount', ascending=False).head(10)
    plt.figure(figsize=(10, 5))
    sns.barplot(x='amount', y='patient_name', data=patient_revenue)
    plt.title('Top 10 Patients by Lifetime Value (LTV)')
    plt.xlabel('Amount Spent')
    plt.ylabel('Patient')
    plt.tight_layout()
    plt.show()

def plot_appointments_by_specialization(df):
    appointments_by_specialization = df.groupby('specialization')['appointment_id'].nunique().reset_index()
    appointments_by_specialization.rename(columns={'appointment_id': 'num_appointments'}, inplace=True)
    appointments_by_specialization = appointments_by_specialization.sort_values('num_appointments', ascending=False)
    plt.figure(figsize=(12, 6))
    sns.barplot(x='num_appointments', y='specialization',
                data=appointments_by_specialization, palette='viridis')
    plt.title('Number of Appointments by Doctor Specialization')
    plt.xlabel('Number of Appointments')
    plt.ylabel('Specialization')
    plt.tight_layout()
    plt.show()

# ============================================================
#  EDA — Run All Plots
# ============================================================

def run_eda(datasets):
    print("\n--- Running EDA ---")
    df = build_analysis_df(datasets)

    # Line Charts
    plot_revenue_trend(df)
    plot_appointments_per_month(df)
    plot_treatment_volume(df)

    # Bar Charts
    plot_top_doctors_by_revenue(df)
    plot_top_patients_by_ltv(df)
    plot_appointments_by_specialization(df)

    print(" EDA complete!")

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

    null_check_after(datasets)
    print("\n Transform complete!")

    # ← EDA runs right after null check
    run_eda(datasets)

main()