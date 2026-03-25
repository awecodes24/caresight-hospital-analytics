from etl.extractor import extract_data
from etl.transform import transform_data
from etl.load import load_data as load_to_db, verify_load
from eda.eda  import run_eda


from logging_monitoring.logger import log_section, setup_logger

logger = setup_logger("main")


def main():
    log_section(logger, "PIPELINE START")

    # Extract
    datasets = extract_data()
    
    # Transform
    datasets = transform_data(datasets)

    # Load
    engine = load_to_db(datasets)

    # Verify
    verify_load(engine)
    
    run_eda(datasets)
    

    log_section(logger, "PIPELINE COMPLETE")

if __name__ == "__main__":
    main()