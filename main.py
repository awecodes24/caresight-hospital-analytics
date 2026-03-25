from transform import main as transform
from load import load_data, verify_load

def main():
    datasets = transform()
    engine = load_data(datasets)
    verify_load(engine, datasets)

if __name__ == "__main__":
    main()