from datetime import datetime

import numpy as np
import pandas as pd

from data_generator.generate_new_users import COUNTRY_LANGUAGE_MAP
from data_generator.utils.utils import COUNTRY_CITY_MAP, random_date


def generate_initial_snapshot(N=1_000_000, zipf_s=1.0):
    cities = [
        (country, city)
        for country, city_list in COUNTRY_CITY_MAP.items()
        for city in city_list
    ]

    ranks = np.arange(1, len(cities) + 1)
    weights = 1 / np.power(ranks, zipf_s)
    probs = weights / weights.sum()

    city_sizes = (probs * N).astype(int)
    city_sizes[0] += N - city_sizes.sum()

    rows = []
    user_id = 1

    for (country, city), size in zip(cities, city_sizes):
        lang = COUNTRY_LANGUAGE_MAP[country]
        for _ in range(size):
            row = {
                "UserID": user_id,
                "FirstName": f"FirstName{user_id}",
                "LastName": f"LastName{user_id}",
                "Email": f"user{user_id}@example.com",
                "Username": f"user_{user_id}",
                "DateOfBirth": random_date().strftime("%Y-%m-%d"),
                "RegistrationDate": datetime(2023, 1, 1).strftime("%Y-%m-%d"),
                "Country": country,
                "City": city,
                "Gender": np.random.choice(["Male", "Female", "Other"], p=[0.48, 0.48, 0.04]),
                "AccountCreatedVia": np.random.choice(["Web", "Mobile", "Partner"], p=[0.5, 0.4, 0.1]),
                "ReferralSource": np.random.choice(["Organic", "Paid", "Referral"], p=[0.5, 0.3, 0.2]),
                "SubscriptionTier": np.random.choice(["Free", "Basic", "Premium"], p=[0.6, 0.3, 0.1]),
                "BillingCycle": np.random.choice(["Monthly", "Annual"], p=[0.75, 0.25]),
                "PaymentMethod": np.random.choice(["Card", "PayPal", "BankTransfer"], p=[0.5, 0.3, 0.2]),
                "AutoRenew": np.random.choice([1, 0], p=[0.7, 0.3]),
                "MarketingConsent": np.random.choice([1, 0], p=[0.6, 0.4]),
                "PreferredLanguage": lang,
                "ContentLanguage": np.random.choice(["en", "de", "fr", "es", "it", "pl", "ro", "nl"], p=[0.3, 0.15, 0.15, 0.1, 0.1, 0.08, 0.07, 0.05]),
                "PlanAddons": np.random.choice(["None", "Sports", "Movies", "Music"], p=[0.5, 0.2, 0.2, 0.1]),
                "TenureDays": 0,
            }
            rows.append(row)
            user_id += 1

    return pd.DataFrame(rows)
