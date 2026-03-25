import matplotlib.pyplot as plt
import seaborn as sns

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