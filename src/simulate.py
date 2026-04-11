from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.clustered_modify import clustered_modify
from src.generate_initial_snapshot import generate_initial_snapshot
from src.generate_new_users import generate_new_users
from src.utils.utils import C_VALUES, GRID, OUTPUT_DIR, R_VALUES, get_rc_for_day, set_seed, weibull_hazard


def simulate(seed=1, N_initial=1000000, save_snapshots=True):
    set_seed(seed)
    start_date = datetime(2023, 1, 1)

    print(f"\n{'=' * 70}", flush=True)
    print(f"Simulation started | seed={seed} | N={N_initial:,}", flush=True)
    print(f"4x4 grid: {len(GRID)} combinations over 100 days", flush=True)
    print(f"r values: {R_VALUES}", flush=True)
    print(f"c values: {C_VALUES}", flush=True)
    print(f"{'=' * 70}", flush=True)

    df = generate_initial_snapshot(N=N_initial)
    if save_snapshots:
        df["SnapshotDate"] = start_date.strftime("%Y-%m-%d")
        df["DayNumber"] = 0
        df["RValue"] = np.nan
        df["CValue"] = np.nan
        df.to_csv(OUTPUT_DIR / "users_day000.csv", index=False)
    print(f"Day   0 | Initial snapshot | Active users: {len(df):>9,}", flush=True)

    for day in range(1, 101):
        r, c = get_rc_for_day(day)

        df["TenureDays"] += 1

        # Calculate hazards with population-dependent churn
        active_user_count = len(df)
        hazards = weibull_hazard(df["TenureDays"].values, active_users=active_user_count)
        survive_mask = np.random.rand(len(df)) >= hazards
        churned = (~survive_mask).sum()
        df = df.loc[survive_mask].reset_index(drop=True)

        df, modified = clustered_modify(df, r, c)

        new_df = generate_new_users(day, df, start_date)
        added = 0
        if new_df is not None:
            df = pd.concat([df, new_df], ignore_index=True)
            added = len(new_df)

        snap_date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
        df["SnapshotDate"] = snap_date
        df["DayNumber"] = day
        df["RValue"] = r
        df["CValue"] = c

        print(
            f"Day {day:>3} | r={r:.3f} c={c:.2f} | "
            f"Active: {len(df):>9,} | "
            f"Churn: {churned:>5,} | "
            f"Modified: {modified:>6,} | "
            f"New: {added:>4,}",
            flush=True
        )

        if save_snapshots:
            df.to_csv(OUTPUT_DIR / f"users_day{day:03d}.csv", index=False)

    print(f"\nDone! 101 snapshots saved to: {OUTPUT_DIR}/", flush=True)
    return df