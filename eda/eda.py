import os
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — no GUI needed
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd

from etl.load import load_datasets_from_db
from logging_monitoring.logger import log_section, setup_logger

logger = setup_logger("eda")

OUTPUT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
#  Helpers
# ============================================================

def _save(fig, filename: str):
    """Save figure as JPEG and close it."""
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, format="jpeg", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved plot → {path}")


# ============================================================
#  Build merged DataFrame
# ============================================================

def build_analysis_df(datasets):
    df = datasets["Billing"].copy()

    df = df.merge(
        datasets["Appointments"][
            ["appointment_id", "patient_id", "doctor_id",
             "appointment_date", "status", "reason_for_visit"]
        ],
        on="patient_id", how="left",
    )
    df = df.merge(
        datasets["Doctors"][
            ["doctor_id", "first_name", "last_name", "specialization"]
        ].rename(columns={"first_name": "doctor_first_name",
                          "last_name":  "doctor_last_name"}),
        on="doctor_id", how="left",
    )
    df = df.merge(
        datasets["Patients"][
            ["patient_id", "first_name", "last_name"]
        ].rename(columns={"first_name": "patient_first_name",
                          "last_name":  "patient_last_name"}),
        on="patient_id", how="left",
    )
    df = df.merge(
        datasets["Treatments"][["treatment_id", "treatment_date", "treatment_type"]],
        on="treatment_id", how="left",
    )

    df["bill_date"]        = pd.to_datetime(df["bill_date"])
    df["appointment_date"] = pd.to_datetime(df["appointment_date"])
    df["treatment_date"]   = pd.to_datetime(df["treatment_date"])
    df["month"]            = df["bill_date"].dt.to_period("M")

    return df



# ============================================================
#  Line charts
# ============================================================

def plot_revenue_trend(df):
    revenue_trend = df.groupby("month")["amount"].sum().reset_index()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(revenue_trend["month"].astype(str), revenue_trend["amount"], marker="o")
    ax.set_title("Hospital Revenue Trend Over Time")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.xticks(rotation=45)
    _save(fig, "revenue_trend.jpeg")



def plot_appointments_per_month(df):
    apm = (df.groupby(df["appointment_date"].dt.to_period("M"))["appointment_id"]
             .nunique()
             .reset_index()
             .rename(columns={"appointment_id": "num_appointments",
                               "appointment_date": "month"}))
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(apm["month"].astype(str), apm["num_appointments"], marker="o")
    ax.set_title("Number of Appointments Per Month")
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of Appointments")
    plt.xticks(rotation=45)
    _save(fig, "appointments_per_month.jpeg")



def plot_treatment_volume(df):
    tvm = (df.groupby(df["treatment_date"].dt.to_period("M"))["treatment_id"]
             .nunique()
             .reset_index()
             .rename(columns={"treatment_id": "num_treatments",
                               "treatment_date": "month"}))
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(tvm["month"].astype(str), tvm["num_treatments"], marker="o", color="orange")
    ax.set_title("Treatment Volume Over Time (Monthly)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of Treatments")
    plt.xticks(rotation=45)
    _save(fig, "treatment_volume.jpeg")


# ============================================================
#  Bar charts
# ============================================================

def plot_top_doctors_by_revenue(df):
    dr = df.groupby(["doctor_first_name", "doctor_last_name"])["amount"].sum().reset_index()
    dr["doctor_name"] = dr["doctor_first_name"] + " " + dr["doctor_last_name"]
    dr = dr.sort_values("amount", ascending=False).head(10)
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(x="amount", y="doctor_name", data=dr, ax=ax)
    ax.set_title("Top 10 Doctors by Revenue")
    ax.set_xlabel("Revenue")
    ax.set_ylabel("Doctor")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    _save(fig, "top_doctors_revenue.jpeg")


def plot_top_patients_by_ltv(df):
    pr = df.groupby(["patient_first_name", "patient_last_name"])["amount"].sum().reset_index()
    pr["patient_name"] = pr["patient_first_name"] + " " + pr["patient_last_name"]
    pr = pr.sort_values("amount", ascending=False).head(10)
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(x="amount", y="patient_name", data=pr, ax=ax)
    ax.set_title("Top 10 Patients by Lifetime Value (LTV)")
    ax.set_xlabel("Amount Spent")
    ax.set_ylabel("Patient")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    _save(fig, "top_patients_ltv.jpeg")


def plot_appointments_by_specialization(df):
    abs_ = (df.groupby("specialization")["appointment_id"]
              .nunique()
              .reset_index()
              .rename(columns={"appointment_id": "num_appointments"})
              .sort_values("num_appointments", ascending=False))
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(x="num_appointments", y="specialization", hue="specialization",
                data=abs_, palette="viridis", legend=False, ax=ax)
    ax.set_title("Appointments by Doctor Specialization")
    ax.set_xlabel("Number of Appointments")
    ax.set_ylabel("Specialization")
    _save(fig, "appointments_by_specialization.jpeg")


# ============================================================
#  Entry point
# ============================================================

def run_eda(engine):
    log_section(logger, "EDA")
    logger.info("Building analysis DataFrame from DB...")

    datasets = load_datasets_from_db(engine)  # ← pulls data from DB
    df = build_analysis_df(datasets)

    logger.info("Generating plots...")
    plot_revenue_trend(df)
    plot_appointments_per_month(df)
    plot_treatment_volume(df)
    plot_top_doctors_by_revenue(df)
    plot_top_patients_by_ltv(df)
    plot_appointments_by_specialization(df)

    logger.info(f"EDA complete! Reports saved to '{OUTPUT_DIR}/'")