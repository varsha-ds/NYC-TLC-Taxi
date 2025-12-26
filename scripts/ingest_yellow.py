import argparse
from nyc_taxi.ingestion.yellow import load_idempotent

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()

    load_idempotent(args.year, args.month)

if __name__ == "__main__":
    main()
