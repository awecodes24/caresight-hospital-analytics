from db.connection import get_connection
import pandas as pd

#  logger
from logging_monitoring.logger import log_section, setup_logger
logger = setup_logger("load")


TABLE_MAP = {
    "Appointments": "appointments",
    "Billing": "bills",
    "Doctors": "doctors",
    "Patients": "patients",
    "Treatments": "treatments",
}



def load_data(datasets):
    log_section(logger, "LOAD")
    logger.info("Starting data load into database...")

    engine = get_connection()
    logger.info(f"Database engine ready: {engine}")

    for name, df in datasets.items():
        table_name = TABLE_MAP[name]

        try:
            logger.info(f"Loading {name} -> {table_name} ({len(df)} rows)")
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )
            logger.info(f"{name} -> '{table_name}' loaded")
        except Exception as e:
            logger.error(f"Failed loading {name}: {e}")

    logger.info("Load complete!")
    return engine



def verify_load(engine):
    log_section(logger, "VERIFY")
    logger.info("Verifying loaded data...")

    for name, table_name in TABLE_MAP.items():
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", con=engine)
        logger.info(f"{name} preview:")
        for line in df.to_string(index=False).splitlines():
            logger.info(line)