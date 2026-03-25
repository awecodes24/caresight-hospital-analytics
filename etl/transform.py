import pandas as pd
from logging_monitoring.logger import setup_logger

logger = setup_logger("transform")

# ============================================================
# TRANSFORM FUNCTION (PIPELINE COMPATIBLE)
# ============================================================

def check_nulls(datasets, stage=""):
    logger.info(f"Null check {stage}:")
    for name, df in datasets.items():
        nulls = df.isnull().sum()
        nulls = nulls[nulls > 0]
        if nulls.empty:
            logger.info(f"  {name}: No nulls")
        else:
            logger.info(f"  {name}:\n{nulls}")


def drop_high_null_rows(datasets, threshold=0.70):
    """
    Drop rows where the weighted proportion of missing values exceeds threshold.
    Weight is calculated as: null_count / total_columns per row.
    """
    logger.info(f"Dropping rows with null weight > {int(threshold * 100)}%...")

    for name, df in datasets.items():
        before = len(df)

        # Calculate null weight per row: number of nulls / total columns
        null_weight = df.isnull().sum(axis=1) / df.shape[1]

        dropped_preview = df[null_weight > threshold]
        if not dropped_preview.empty:
            logger.warning(f"  {name}: {len(dropped_preview)} rows exceed {int(threshold*100)}% null threshold:")
            for idx, weight in null_weight[null_weight > threshold].items():
                logger.warning(f"    Row {idx} — null weight: {weight:.0%}")

        datasets[name] = df[null_weight <= threshold].reset_index(drop=True)

        after = len(datasets[name])
        dropped = before - after
        if dropped > 0:
            logger.warning(f"  {name}: Dropped {dropped} rows (null weight > {int(threshold*100)}%)")
        else:
            logger.info(f"  {name}: No rows dropped")

    return datasets


def transform_data(datasets):
    logger.info("Starting transformation process...")

    # -------------------------------
    # NULL CHECK BEFORE
    # -------------------------------
    check_nulls(datasets, stage="BEFORE TRANSFORM")

    # -------------------------------
    # DROP ROWS WITH >70% NULL WEIGHT
    # -------------------------------
    datasets = drop_high_null_rows(datasets, threshold=0.70)

    # -------------------------------
    # FIX DTYPES
    # -------------------------------
    logger.info("Fixing data types...")

    # Appointments
    datasets["Appointments"]['appointment_date'] = pd.to_datetime(
        datasets["Appointments"]['appointment_date'], errors='coerce')
    datasets["Appointments"]['appointment_time'] = pd.to_datetime(
        datasets["Appointments"]['appointment_time'].astype(str).str.strip(),
        format='mixed', errors='coerce'
    ).dt.time

    # Billing
    datasets["Billing"]['bill_date'] = pd.to_datetime(
        datasets["Billing"]['bill_date'], errors='coerce')
    datasets["Billing"]['amount'] = pd.to_numeric(
        datasets["Billing"]['amount'], errors='coerce')

    # Doctors
    datasets["Doctors"]['phone_number'] = pd.to_numeric(
        datasets["Doctors"]['phone_number'], errors='coerce')
    datasets["Doctors"]['years_experience'] = pd.to_numeric(
        datasets["Doctors"]['years_experience'], errors='coerce')
    datasets["Doctors"] = datasets["Doctors"].assign(                         # ← ADD
        specialization=datasets["Doctors"]['specialization'].str.strip().str.title()
    )

    # Patients
    datasets["Patients"]['date_of_birth'] = pd.to_datetime(
        datasets["Patients"]['date_of_birth'], errors='coerce')
    datasets["Patients"]['registration_date'] = pd.to_datetime(
        datasets["Patients"]['registration_date'], errors='coerce')
    datasets["Patients"]['contact_number'] = pd.to_numeric(
        datasets["Patients"]['contact_number'], errors='coerce')
    datasets["Patients"] = datasets["Patients"].assign(                       # ← ADD
        gender=datasets["Patients"]['gender'].str.strip().str.upper()
    )

    # Treatments
    datasets["Treatments"]['cost'] = pd.to_numeric(
        datasets["Treatments"]['cost'], errors='coerce')
    datasets["Treatments"]['treatment_date'] = pd.to_datetime(
        datasets["Treatments"]['treatment_date'], errors='coerce')
    
    # -------------------------------
    # FILL MISSING VALUES (CoW-safe)
    # -------------------------------
    logger.info("Filling missing values...")

    datasets["Appointments"] = datasets["Appointments"].fillna({
        'doctor_id':        'Unknown',
        'reason_for_visit': 'Not Specified',
        'status':           'Unknown'
    })

    datasets["Billing"] = datasets["Billing"].assign(
        amount=datasets["Billing"]['amount'].fillna(datasets["Billing"]['amount'].median())
    )

    datasets["Doctors"] = datasets["Doctors"].assign(
        years_experience=datasets["Doctors"]['years_experience'].fillna(
            datasets["Doctors"]['years_experience'].median()
        ),
        phone_number=datasets["Doctors"]['phone_number'].fillna(
            datasets["Doctors"]['phone_number'].median()
        ),
        email=datasets["Doctors"]['email'].fillna('Not Provided')
    )

    datasets["Patients"] = datasets["Patients"].assign(
        gender=datasets["Patients"]['gender'].fillna('Unknown'),
        address=datasets["Patients"]['address'].fillna('Not Provided'),
        date_of_birth=datasets["Patients"]['date_of_birth'].fillna(
            datasets["Patients"]['date_of_birth'].mode()[0]
        ),
        contact_number=datasets["Patients"]['contact_number'].fillna(
            datasets["Patients"]['contact_number'].median()
        ),
        registration_date=datasets["Patients"]['registration_date'].fillna(
            datasets["Patients"]['registration_date'].mode()[0]
        ),
        insurance_provider=datasets["Patients"]['insurance_provider'].fillna('Unknown'),
        insurance_number=datasets["Patients"]['insurance_number'].fillna('Unknown'),
        email=datasets["Patients"]['email'].fillna('Not Provided')
    )

    datasets["Treatments"] = datasets["Treatments"].assign(
        cost=datasets["Treatments"]['cost'].fillna(
            datasets["Treatments"]['cost'].median()
        ),
        treatment_type=datasets["Treatments"]['treatment_type'].fillna('Unknown'),
        description=datasets["Treatments"]['description'].fillna('Not Provided'),
        treatment_date=datasets["Treatments"]['treatment_date'].fillna(
            datasets["Treatments"]['treatment_date'].mode()[0]
        )
    )

    # -------------------------------
    # DROP REMAINING NULLS (critical tables only)
    # -------------------------------
    logger.warning("Dropping remaining nulls in critical tables...")

    for name in ["Appointments", "Billing"]:
        before = len(datasets[name])
        datasets[name] = datasets[name].dropna().reset_index(drop=True)
        dropped = before - len(datasets[name])
        if dropped > 0:
            logger.warning(f"  {name}: Dropped {dropped} remaining null rows")

    # -------------------------------
    # FINAL NULL CHECK
    # -------------------------------
    check_nulls(datasets, stage="AFTER TRANSFORM")

    for name, df in datasets.items():
        total_nulls = df.isnull().sum().sum()
        if total_nulls == 0:
            logger.info(f"  {name}: Clean ✓")
        else:
            logger.warning(f"  {name}: {total_nulls} nulls remain")

    logger.info("Transformation complete!")
    return datasets