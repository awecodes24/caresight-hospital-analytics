import pandas as pd

from logging_monitoring.logger import log_section, setup_logger
logger = setup_logger("transform")



def transform_data(datasets):
    log_section(logger, "TRANSFORM")
    logger.info("Starting transformation process...")

    for name, df in datasets.items():
        logger.info(f"{name}: {len(df)} rows (pre-transform)")

 
    logger.info("Fixing data types...")

    df = datasets["Appointments"]
    df['appointment_date'] = pd.to_datetime(df['appointment_date'], errors='coerce')
    df['appointment_time'] = pd.to_datetime(
        df['appointment_time'].astype(str).str.strip(),
        format='mixed',
        errors='coerce'
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


    logger.info("Filling missing values...")

    datasets["Appointments"].fillna({
        'doctor_id'       : 'Unknown',
        'reason_for_visit': 'Not Specified',
        'status'          : 'Unknown'
    }, inplace=True)

    datasets["Billing"]['amount'] = datasets["Billing"]['amount'].fillna(
        datasets["Billing"]['amount'].median()
    )

    datasets["Doctors"]['years_experience'] = datasets["Doctors"]['years_experience'].fillna(
        datasets["Doctors"]['years_experience'].median()
    )

    datasets["Patients"].fillna({
        'gender' : 'Unknown',
        'address': 'Not Provided'
    }, inplace=True)

    datasets["Treatments"]['cost'] = datasets["Treatments"]['cost'].fillna(
        datasets["Treatments"]['cost'].median()
    )


    logger.warning("Dropping remaining nulls...")

    for name in ["Appointments", "Billing"]:
        before = len(datasets[name])
        datasets[name] = datasets[name].dropna()
        after = len(datasets[name])
        logger.warning(f"{name}: Dropped {before - after} rows")

    logger.info("Transformation complete!")
    for name, df in datasets.items():
        logger.info(f"{name}: {len(df)} rows (post-transform)")
    return datasets