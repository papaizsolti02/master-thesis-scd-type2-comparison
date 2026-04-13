from datetime import datetime, timedelta
import random
from pathlib import Path
from itertools import product

import numpy as np

# ==================== Output directory for snapshots ====================
# Output directory for snapshots
OUTPUT_DIR = Path("snapshots")
OUTPUT_DIR.mkdir(exist_ok=True)

# ==================== R and C values for the 4x4 grid ====================
R_VALUES = [0.003, 0.010, 0.040, 0.150]
C_VALUES = [0.02, 0.10, 0.30, 1.0]
GRID = list(product(R_VALUES, C_VALUES))

# ==================== Get R and C for a given day based on the 4x4 grid ====================
def get_rc_for_day(day):
    idx = (day - 1) % len(GRID)
    return GRID[idx]

# ==================== Weibull parameters for churn simulation ====================
WEIBULL_K = 0.7
WEIBULL_LAMBDA = 1 / (730 ** WEIBULL_K)

# ==================== Weibull hazard function with population-dependent churn ====================
def weibull_hazard(T, active_users=None, alpha=0.05):
    """Calculate Weibull hazard rate with optional population-dependent churn.
    
    Args:
        T: Tenure in days (scalar or array)
        active_users: Current active user count. If None, hazard is age-based only.
        alpha: Sensitivity to population effects (default 0.05)
    
    Returns:
        Hazard rate(s), adjusted for population if active_users provided.
    """
    base_hazard = WEIBULL_K * WEIBULL_LAMBDA * np.power(T, WEIBULL_K - 1)
    
    if active_users is not None and active_users > 0:
        # Network effect: larger user base → lower churn
        # multiplier = 1 - α × log(active_users)
        # At 1M users: multiplier ≈ 1 - 0.05 × 13.8 ≈ 0.31 (low churn)
        # At 100K users: multiplier ≈ 1 - 0.05 × 11.5 ≈ 0.42 (medium churn)
        # At 10K users: multiplier ≈ 1 - 0.05 × 9.2 ≈ 0.54 (higher churn)
        log_users = np.log(active_users)
        multiplier = 1 - alpha * log_users
        multiplier = np.clip(multiplier, 0.1, 1.0)  # Bound between 0.1 and 1.0
        return base_hazard * multiplier
    
    return base_hazard

# ==================== Set random seeds for reproducibility ====================
def set_seed(seed):
    np.random.seed(seed)
    random.seed(seed)


# ==================== Country to city mapping for user generation ====================
COUNTRY_CITY_MAP = {
    "Germany": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
    "Italy": ["Rome", "Milan", "Naples", "Turin", "Bologna"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"],
    "Netherlands": ["Amsterdam", "Rotterdam", "Utrecht", "Eindhoven"],
    "Poland": ["Warsaw", "Krakow", "Gdansk", "Wroclaw"],
    "Romania": ["Bucharest", "Cluj-Napoca", "Timisoara", "Iasi"],
}

COUNTRY_LANGUAGE_MAP = {
    "Germany": "de",
    "France": "fr",
    "United Kingdom": "en",
    "Italy": "it",
    "Spain": "es",
    "Netherlands": "nl",
    "Poland": "pl",
    "Romania": "ro",
}

# ==================== Generate a random date of birth for new users ====================
def random_date(start_year=1960, end_year=2003):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))
