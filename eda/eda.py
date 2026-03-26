import os
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — no GUI needed
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd
import numpy as np

from etl.load import load_datasets_from_db
from logging_monitoring.logger import log_section, setup_logger

logger = setup_logger("eda")

OUTPUT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Global style
sns.set_theme(style="whitegrid", palette="muted")



# Helper

def _save(fig, filename: str):
    """Save figure as JPEG and close it."""
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, format="jpeg", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved plot → {path}")


#  Build merged DataFrame

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
            ["patient_id", "first_name", "last_name", "date_of_birth", "gender"]
        ].rename(columns={"first_name": "patient_first_name",
                          "last_name":  "patient_last_name"}),
        on="patient_id", how="left",
    )
    df = df.merge(
        datasets["Treatments"][["treatment_id", "treatment_date", "treatment_type"]],
        on="treatment_id", how="left",
    )

    df["bill_date"]        = pd.to_datetime(df["bill_date"],        errors="coerce")
    df["appointment_date"] = pd.to_datetime(df["appointment_date"], errors="coerce")
    df["treatment_date"]   = pd.to_datetime(df["treatment_date"],   errors="coerce")
    df["date_of_birth"]    = pd.to_datetime(df["date_of_birth"],    errors="coerce")
    df["month"]            = df["bill_date"].dt.to_period("M")

    # Derive age from date_of_birth
    today = pd.Timestamp("today")
    df["age"] = ((today - df["date_of_birth"]).dt.days / 365.25).round(1)
    df["age"] = df["age"].where(df["age"].between(0, 120))  # sanity filter

    return df


#  1. LINE CHART — Revenue Trend Over Time

def plot_revenue_trend(df):
    revenue_trend = df.groupby("month")["amount"].sum().reset_index()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(revenue_trend["month"].astype(str), revenue_trend["amount"],
            marker="o", linewidth=2, color="#2196F3")
    ax.fill_between(range(len(revenue_trend)), revenue_trend["amount"],
                    alpha=0.1, color="#2196F3")
    ax.set_title("Hospital Revenue Trend Over Time", fontsize=14, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.xticks(range(len(revenue_trend)),
               revenue_trend["month"].astype(str), rotation=45, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    _save(fig, "01_revenue_trend.jpeg")


#  2. LINE CHART — New Patients Registered Per Month

def plot_new_patients_trend(datasets):
    patients = datasets["Patients"].copy()
    patients["registration_date"] = pd.to_datetime(
        patients["registration_date"], errors="coerce")
    patients["month"] = patients["registration_date"].dt.to_period("M")
    monthly = patients.groupby("month").size().reset_index(name="new_patients")

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(monthly["month"].astype(str), monthly["new_patients"],
            marker="s", linewidth=2, color="#4CAF50")
    ax.fill_between(range(len(monthly)), monthly["new_patients"],
                    alpha=0.1, color="#4CAF50")
    ax.set_title("New Patient Registrations Per Month", fontsize=14, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of New Patients")
    plt.xticks(range(len(monthly)),
               monthly["month"].astype(str), rotation=45, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    _save(fig, "02_new_patients_trend.jpeg")


#  3. BAR CHART — Top 10 Doctors by Revenue

def plot_top_doctors_by_revenue(df):
    dr = df.groupby(["doctor_first_name", "doctor_last_name"])["amount"].sum().reset_index()
    dr["doctor_name"] = dr["doctor_first_name"] + " " + dr["doctor_last_name"]
    dr = dr.sort_values("amount", ascending=False).head(10)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(dr["doctor_name"], dr["amount"], color="#5C6BC0", edgecolor="white")
    for bar, val in zip(bars, dr["amount"]):
        ax.text(bar.get_width() + 500, bar.get_y() + bar.get_height() / 2,
                f"${val:,.0f}", va="center", fontsize=8)
    ax.set_title("Top 10 Doctors by Revenue", fontsize=14, fontweight="bold")
    ax.set_xlabel("Revenue")
    ax.set_ylabel("Doctor")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.invert_yaxis()
    plt.tight_layout()
    _save(fig, "03_top_doctors_revenue.jpeg")


#  4. BAR CHART — Appointments by Specialization

def plot_appointments_by_specialization(df):
    abs_ = (df.groupby("specialization")["appointment_id"]
              .nunique()
              .reset_index()
              .rename(columns={"appointment_id": "num_appointments"})
              .sort_values("num_appointments", ascending=False))

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(x="num_appointments", y="specialization", hue="specialization",
                data=abs_, palette="viridis", legend=False, ax=ax)
    ax.set_title("Appointments by Doctor Specialization", fontsize=14, fontweight="bold")
    ax.set_xlabel("Number of Appointments")
    ax.set_ylabel("Specialization")
    plt.tight_layout()
    _save(fig, "04_appointments_by_specialization.jpeg")



#  5. STACKED BAR — Payment Status by Payment Method

def plot_payment_status_stacked(datasets):
    billing = datasets["Billing"].copy()
    billing = billing.dropna(subset=["payment_method", "payment_status"])

    pivot = (billing.groupby(["payment_method", "payment_status"])
                    .size()
                    .unstack(fill_value=0))

    colors = ["#66BB6A", "#EF5350", "#FFA726", "#42A5F5", "#AB47BC"]
    fig, ax = plt.subplots(figsize=(12, 6))
    pivot.plot(kind="bar", stacked=True, ax=ax,
               color=colors[:len(pivot.columns)], edgecolor="white", width=0.6)
    ax.set_title("Payment Status Breakdown by Payment Method",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Payment Method")
    ax.set_ylabel("Number of Bills")
    ax.legend(title="Payment Status", bbox_to_anchor=(1.01, 1), loc="upper left")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    _save(fig, "05_payment_status_stacked.jpeg")


#  6. HEATMAP — Appointments by Day of Week & Hour

def plot_appointment_heatmap(datasets):
    appts = datasets["Appointments"].copy()
    appts["appointment_date"] = pd.to_datetime(
        appts["appointment_date"], errors="coerce")
    appts["appointment_time"] = pd.to_datetime(
        appts["appointment_time"].astype(str), format="mixed", errors="coerce")

    appts = appts.dropna(subset=["appointment_date", "appointment_time"])
    appts["day_of_week"] = appts["appointment_date"].dt.day_name()
    appts["hour"] = pd.to_datetime(
        appts["appointment_time"].astype(str), errors="coerce").dt.hour

    appts = appts.dropna(subset=["hour"])
    appts["hour"] = appts["hour"].astype(int)

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot = (appts.groupby(["day_of_week", "hour"])
                  .size()
                  .unstack(fill_value=0)
                  .reindex(day_order))

    fig, ax = plt.subplots(figsize=(14, 6))
    sns.heatmap(pivot, cmap="YlOrRd", linewidths=0.3, annot=False,
                ax=ax, cbar_kws={"label": "Appointments"})
    ax.set_title("Appointment Heatmap — Day of Week vs Hour of Day",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Day of Week")
    plt.tight_layout()
    _save(fig, "06_appointment_heatmap.jpeg")



#  7. HISTOGRAM — Distribution of Bill Amounts

def plot_billing_distribution(datasets):
    billing = datasets["Billing"].copy()
    billing = billing.dropna(subset=["amount"])

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(billing["amount"], bins=40, color="#29B6F6",
            edgecolor="white", linewidth=0.5)
    ax.axvline(billing["amount"].mean(), color="#E53935",
               linestyle="--", linewidth=1.5, label=f'Mean: ${billing["amount"].mean():,.0f}')
    ax.axvline(billing["amount"].median(), color="#43A047",
               linestyle="--", linewidth=1.5, label=f'Median: ${billing["amount"].median():,.0f}')
    ax.set_title("Distribution of Bill Amounts", fontsize=14, fontweight="bold")
    ax.set_xlabel("Bill Amount ($)")
    ax.set_ylabel("Frequency")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend()
    plt.tight_layout()
    _save(fig, "07_billing_distribution.jpeg")



#  8. SCATTER PLOT — Patient Age vs Bill Amount

def plot_age_vs_billing(df):
    scatter_df = df.dropna(subset=["age", "amount", "specialization"])
    # Limit to top 6 specializations for readability
    top_specs = (scatter_df.groupby("specialization")["amount"]
                           .sum()
                           .nlargest(6)
                           .index)
    scatter_df = scatter_df[scatter_df["specialization"].isin(top_specs)]

    fig, ax = plt.subplots(figsize=(11, 6))
    for spec, grp in scatter_df.groupby("specialization"):
        ax.scatter(grp["age"], grp["amount"], label=spec, alpha=0.5, s=20)

    ax.set_title("Patient Age vs Bill Amount by Specialization",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Patient Age (years)")
    ax.set_ylabel("Bill Amount ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(title="Specialization", bbox_to_anchor=(1.01, 1), loc="upper left",
              fontsize=8)
    plt.tight_layout()
    _save(fig, "08_age_vs_billing_scatter.jpeg")



#  9. BOX PLOT — Age Distribution by Gender

def plot_age_by_gender_boxplot(df):
    box_df = df.dropna(subset=["age", "gender"]).copy()

    # Normalize gender labels
    box_df["gender"] = box_df["gender"].str.strip().str.upper()
    box_df = box_df[box_df["gender"].isin(["M", "F"])]
    box_df["gender"] = box_df["gender"].map({"M": "Male", "F": "Female"})

    # One row per patient to avoid duplicates from billing joins
    box_df = box_df.drop_duplicates(subset=["patient_id"])

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.boxplot(x="gender", y="age", hue="gender",
                data=box_df, palette={"Male": "#42A5F5", "Female": "#EC407A"},
                width=0.45, linewidth=1.5, legend=False, ax=ax)
    sns.stripplot(x="gender", y="age", data=box_df,
                  color="black", alpha=0.2, size=2.5, jitter=True, ax=ax)

    ax.set_title("Age Distribution by Gender (Male vs Female Patients)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Gender")
    ax.set_ylabel("Age (years)")

    # Annotate median values
    for i, gender in enumerate(["Female", "Male"]):
        median_val = box_df[box_df["gender"] == gender]["age"].median()
        ax.text(i, median_val + 1.5, f"Median: {median_val:.1f}",
                ha="center", fontsize=9, color="black", fontweight="bold")

    plt.tight_layout()
    _save(fig, "09_age_by_gender_boxplot.jpeg")


#  Entry point

def run_eda(engine):
    log_section(logger, "EDA")
    logger.info("Building analysis DataFrame from DB...")

    datasets = load_datasets_from_db(engine)
    df = build_analysis_df(datasets)

    logger.info("Generating plots...")

    # Line Charts
    plot_revenue_trend(df)
    plot_new_patients_trend(datasets)

    # Bar Charts
    plot_top_doctors_by_revenue(df)
    plot_appointments_by_specialization(df)

    # Stacked Bar
    plot_payment_status_stacked(datasets)

    # Heatmap
    plot_appointment_heatmap(datasets)

    # Histogram
    plot_billing_distribution(datasets)

    # Scatter (advanced)
    plot_age_vs_billing(df)

    # Box Plot
    plot_age_by_gender_boxplot(df)

    logger.info(f"EDA complete! {9} reports saved to '{OUTPUT_DIR}/'")