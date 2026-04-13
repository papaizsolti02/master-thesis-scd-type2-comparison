from datetime import datetime, timedelta
from random import random

import numpy as np
import pandas as pd

from data_generator.utils.utils import COUNTRY_LANGUAGE_MAP, random_date


def generate_new_users(day, df, start_date):
    lam = max(10, 200 + 50 * np.sin(2 * np.pi * day / 30))
    num_new = np.random.poisson(lam)
    if num_new == 0:
        return None

    cities = df[["Country", "City"]].drop_duplicates().values
    chosen = cities[np.random.randint(0, len(cities), num_new)]
    max_id = df["UserID"].max()
    reg_date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")

    new_rows = []
    for i in range(num_new):
        country = chosen[i][0]
        city = chosen[i][1]
        lang = COUNTRY_LANGUAGE_MAP.get(country, "en")
        uid = max_id + 1 + i
        row = {
            "UserID": uid,
            "FirstName": f"FirstName{uid}",
            "LastName": f"LastName{uid}",
            "Email": f"user{uid}@example.com",
            "Username": f"user_{uid}",
            "DateOfBirth": random_date().strftime("%Y-%m-%d"),
            "RegistrationDate": reg_date,
            "Country": country,
            "City": city,
            "Gender": np.random.choice(["Male", "Female", "Other"], p=[0.48, 0.48, 0.04]),
            "AccountCreatedVia": np.random.choice(["Web", "Mobile", "Partner"], p=[0.5, 0.4, 0.1]),
            "ReferralSource": np.random.choice(["Organic", "Paid", "Referral"], p=[0.5, 0.3, 0.2]),
            "SubscriptionTier": "Free",
            "BillingCycle": "Monthly",
            "PaymentMethod": "Card",
            "AutoRenew": 1,
            "MarketingConsent": 1,
            "PreferredLanguage": lang,
            "ContentLanguage": "en",
            "PlanAddons": "None",
            "TenureDays": 0,
        }
        new_rows.append(row)

    return pd.DataFrame(new_rows)
