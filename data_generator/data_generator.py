from pathlib import Path
from data_generator.simulate import simulate
from data_generator.adls.upload_snapshots import upload_snapshots
from data_generator.utils.utils import OUTPUT_DIR


if __name__ == "__main__":
    simulate(seed=1, N_initial=100000, save_snapshots=True)
    uploaded_files = upload_snapshots(OUTPUT_DIR)
    print(f"ADLS upload done. Uploaded files: {uploaded_files}", flush=True)
