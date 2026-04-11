from pathlib import Path
from src.simulate import simulate
from src.adls.upload_snapshots import upload_snapshots
from src.utils.utils import OUTPUT_DIR


if __name__ == "__main__":
    simulate(seed=1, N_initial=10000, save_snapshots=True)
    uploaded_files = upload_snapshots(OUTPUT_DIR)
    print(f"ADLS upload done. Uploaded files: {uploaded_files}", flush=True)
