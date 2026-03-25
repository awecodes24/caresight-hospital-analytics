import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from logging_monitoring.logger import setup_logger

logger = setup_logger("eda")

# ============================================================
# BUILD ANALYSIS DATAFRAME
# ============================================================

def build_analysis_df(datasets):
    logger.info("Building analysis dataframe...")

    df = datasets["Billing"].copy()

    df = df.merge(
        datasets["Appointments"][[
            'appointment_id','patient_id','doctor_id',
            'appointment_date','status','reason_for_visit'
        ]],
        on='patient_id', how='left'
    )

    df = df.merge(
        datasets["Doctors"][['doctor_id','first_name','last_name','specialization']]
        .rename(columns={
            'first_name':'doctor_first_name',
            'last_name':'doctor_last_name'
        }),
        on='doctor_id', how='left'
    )

    df = df.merge(
        datasets["Patients"][['patient_id','first_name','last_name']]
        .rename(columns={
            'first_name':'patient_first_name',
            'last_name':'patient_last_name'
        }),
        on='patient_id', how='left'
    )

    df = df.merge(
        datasets["Treatments"][['treatment_id','treatment_date','treatment_type']],
        on='treatment_id', how='left'
    )

    df['month'] = pd.to_datetime(df['bill_date']).dt.to_period('M')

    logger.info(f"Analysis dataframe created: {len(df)} rows")

    return df


# ============================================================
# PLOTS
# ============================================================

def plot_revenue_trend(df):
    revenue_trend = df.groupby('month')['amount'].sum().reset_index()

    plt.figure()
    plt.plot(revenue_trend['month'].astype(str), revenue_trend['amount'])
    plt.title('Revenue Trend')
    plt.xticks(rotation=45)
    plt.show()


def plot_appointments_per_month(df):
    appt = df.groupby(
        df['appointment_date'].dt.to_period('M')
    )['appointment_id'].nunique().reset_index()

    plt.figure()
    plt.plot(appt['appointment_date'].astype(str), appt['appointment_id'])
    plt.title('Appointments per Month')
    plt.xticks(rotation=45)
    plt.show()


def plot_treatment_volume(df):
    treat = df.groupby(
        df['treatment_date'].dt.to_period('M')
    )['treatment_id'].nunique().reset_index()

    plt.figure()
    plt.plot(treat['treatment_date'].astype(str), treat['treatment_id'])
    plt.title('Treatment Volume')
    plt.xticks(rotation=45)
    plt.show()


def plot_top_doctors_by_revenue(df):
    doctor_rev = df.groupby(
        ['doctor_first_name','doctor_last_name']
    )['amount'].sum().reset_index()

    doctor_rev['doctor_name'] = (
        doctor_rev['doctor_first_name'] + " " + doctor_rev['doctor_last_name']
    )

    doctor_rev = doctor_rev.sort_values('amount', ascending=False).head(10)

    plt.figure()
    sns.barplot(x='amount', y='doctor_name', data=doctor_rev)
    plt.title('Top Doctors by Revenue')
    plt.show()


# ============================================================
# MAIN EDA FUNCTION
# ============================================================

def run_eda(datasets):
    summary = {}

    for name, df in datasets.items():
        summary[name] = {
            "rows": df.shape[0],
            "cols": df.shape[1],
            "missing": df.isnull().sum().to_dict(),
            "describe": df.describe().to_dict()
        }

    logger.info("EDA complete!")
    return summary
    

    