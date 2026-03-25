import pandas as pd
from pathlib import Path

#logger
from logging_monitoring.logger import log_section, setup_logger
logger = setup_logger("extract")



BASE_PATH = Path(__file__).resolve().parent / "hospital_etl_datasources"



def extract_data():
    log_section(logger, "EXTRACT")
    logger.info("Starting data extraction from CSV sources...")

    df_appoint = pd.read_csv(BASE_PATH / "appointments_etl.csv")
    logger.info(f"Appointments: {len(df_appoint)} rows")

    df_bill = pd.read_csv(BASE_PATH / "billing_etl.csv")
    logger.info(f"Billing: {len(df_bill)} rows")

    df_doctors = pd.read_csv(BASE_PATH / "doctors_etl.csv")
    logger.info(f"Doctors: {len(df_doctors)} rows")

    df_patient = pd.read_csv(BASE_PATH / "patients_etl.csv")
    logger.info(f"Patients: {len(df_patient)} rows")

    df_treatment = pd.read_csv(BASE_PATH / "treatments_etl.csv")
    logger.info(f"Treatments: {len(df_treatment)} rows")

    logger.info("All CSV files loaded successfully")

    datasets = {
        "Appointments": df_appoint,
        "Billing": df_bill,
        "Doctors": df_doctors,
        "Patients": df_patient,
        "Treatments": df_treatment,
    }

    return datasets