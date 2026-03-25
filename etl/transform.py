import pandas as pd
from logging_monitoring.logger import setup_logger

logger = setup_logger("transform")

# ============================================================
# TRANSFORM FUNCTION (PIPELINE COMPATIBLE)
# ============================================================

def transform_data(datasets):
    logger.info("Starting transformation process...")

    # -------------------------------
    # NULL CHECK BEFORE
    # -------------------------------
    logger.info("Checking nulls before transform...")
    for name, df in datasets.items():
        logger.info(f"{name} nulls:\n{df.isnull().sum()}")

    # -------------------------------
    # FIX DTYPES
    # -------------------------------
    logger.info("Fixing data types...")

    # Appointments
    df = datasets["Appointments"]
    df['appointment_date'] = pd.to_datetime(df['appointment_date'], errors='coerce')
    df['appointment_time'] = pd.to_datetime(
        df['appointment_time'].astype(str).str.strip(),
        format='mixed',
        errors='coerce'
    ).dt.time

    # Billing
    df = datasets["Billing"]
    df['bill_date'] = pd.to_datetime(df['bill_date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Doctors
    df = datasets["Doctors"]
    df['phone_number'] = pd.to_numeric(df['phone_number'], errors='coerce')
    df['years_experience'] = pd.to_numeric(df['years_experience'], errors='coerce')

    # Patients
    df = datasets["Patients"]
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
    df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
    df['contact_number'] = pd.to_numeric(df['contact_number'], errors='coerce')

    # Treatments
    df = datasets["Treatments"]
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
    df['treatment_date'] = pd.to_datetime(df['treatment_date'], errors='coerce')

    # -------------------------------
    # FILL MISSING VALUES
    # -------------------------------
    logger.info("Filling missing values...")

    datasets["Appointments"].fillna({
        'doctor_id': 'Unknown',
        'reason_for_visit': 'Not Specified',
        'status': 'Unknown'
    }, inplace=True)

    datasets["Billing"]['amount'].fillna(
        datasets["Billing"]['amount'].median(), inplace=True
    )

    datasets["Doctors"]['years_experience'].fillna(
        datasets["Doctors"]['years_experience'].median(), inplace=True
    )

    datasets["Patients"].fillna({
        'gender': 'Unknown',
        'address': 'Not Provided'
    }, inplace=True)

    datasets["Treatments"]['cost'].fillna(
        datasets["Treatments"]['cost'].median(), inplace=True
    )

    # -------------------------------
    # DROP REMAINING NULLS
    # -------------------------------
    logger.warning("Dropping remaining nulls...")

    for name in ["Appointments", "Billing"]:
        before = len(datasets[name])
        datasets[name] = datasets[name].dropna()
        after = len(datasets[name])
        dropped = before - after

        if dropped > 0:
            logger.warning(f"{name}: Dropped {dropped} rows")

    # -------------------------------
    # FINAL NULL CHECK
    # -------------------------------
    logger.info("Final null check...")

    for name, df in datasets.items():
        total_nulls = df.isnull().sum().sum()
        if total_nulls == 0:
            logger.info(f"{name}: Clean")
        else:
            logger.warning(f"{name}: {total_nulls} nulls remain")

    logger.info("Transformation complete!")

    return datasets