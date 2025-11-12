import subprocess
import time

def run_step(cmd, name):
    print(f"\nRunning {name}...")
    start = time.time()
    subprocess.run(cmd, shell=True, check=True)
    print(f"{name} done in {time.time() - start:.1f}s")

if __name__ == "__main__":
    print("Starting ETL pipeline...")

    run_step("python scraper.py", "scraper")
    run_step("python processing/processing.py", "data processing")
    run_step("python processing/load_to_postgres.py", "load to postgres")

    print("Pipeline complete.")
