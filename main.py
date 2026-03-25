import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etl.extractor import extract_data
from etl.transform import transform_data

from etl.load import load_data, verify_load, load_datasets_from_db
from eda.eda import run_eda

from logging_monitoring.logger import log_section, setup_logger

logger = setup_logger("main")


def main():
    log_section(logger, "PIPELINE START")

    # Extract
    logger.info("Starting extraction...")
    datasets = extract_data()
    logger.info("Extraction complete.")

    # Transform
    logger.info("Starting transformation...")
    datasets = transform_data(datasets)
    logger.info("Transformation complete.")

    # Load
    logger.info("Starting load into database...")
    engine = load_data(datasets)
    logger.info("Load complete.")

    # Verify
    logger.info("Verifying loaded data...")
    verify_load(engine)
    logger.info("Verification complete.")

    # EDA
    logger.info("Starting EDA...")
    run_eda(engine)
    logger.info("EDA complete.")

    log_section(logger, "PIPELINE COMPLETE")


if __name__ == "__main__":
    main()