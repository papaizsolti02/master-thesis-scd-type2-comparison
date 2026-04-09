from pathlib import Path
from src.simulate import simulate
from src.upload_snapshots_to_adls import upload_snapshots_to_adls
from src.utils.utils import OUTPUT_DIR


if __name__ == "__main__":
    simulate(seed=1, N_initial=1000000, save_snapshots=True)
    uploaded_files = upload_snapshots_to_adls(OUTPUT_DIR)
    print(f"ADLS upload done. Uploaded files: {uploaded_files}", flush=True)
