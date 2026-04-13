import numpy as np


def _sample_different(current_values, candidate_values):
    """Sample replacement values that differ from current row values."""
    result = np.array(current_values, copy=True)
    for i, current in enumerate(current_values):
        alternatives = [v for v in candidate_values if v != current]
        if alternatives:
            result[i] = np.random.choice(alternatives)
    return result


def clustered_modify(df, r, c):
    N = len(df)
    total_modify = int(r * N)
    if total_modify == 0:
        return df, 0

    city_counts = df["City"].value_counts()
    num_buckets = max(1, int(c * len(city_counts)))
    probs = city_counts / city_counts.sum()

    selected_cities = np.random.choice(
        probs.index, size=num_buckets, replace=False, p=probs.values
    )

    selected_counts = city_counts[selected_cities]
    proportions = selected_counts / selected_counts.sum()
    changes_per_city = (proportions * total_modify).astype(int)
    changes_per_city.iloc[0] += total_modify - changes_per_city.sum()

    modify_indices = []
    for city, n in changes_per_city.items():
        city_idx = df.index[df["City"] == city].to_numpy()
        n = min(n, len(city_idx))
        if n == 0:
            continue
        chosen = np.random.choice(city_idx, size=n, replace=False)
        modify_indices.append(chosen)

    if not modify_indices:
        return df, 0

    modify_indices = np.concatenate(modify_indices)

    attr_values = {
        "SubscriptionTier": (["Free", "Basic", "Premium"], 0.20),
        "BillingCycle": (["Monthly", "Annual"], 0.10),
        "PaymentMethod": (["Card", "PayPal", "BankTransfer"], 0.10),
        "AutoRenew": ([1, 0], 0.10),
        "MarketingConsent": ([1, 0], 0.10),
        "PreferredLanguage": (["en", "de", "fr", "es", "it", "pl", "ro", "nl"], 0.10),
        "ContentLanguage": (["en", "de", "fr", "es", "it", "pl", "ro", "nl"], 0.15),
        "PlanAddons": (["None", "Sports", "Movies", "Music"], 0.15),
    }

    attrs = np.random.choice(
        list(attr_values.keys()),
        size=len(modify_indices),
        p=[v[1] for v in attr_values.values()],
    )

    for attr, (values, _) in attr_values.items():
        mask = modify_indices[attrs == attr]
        if len(mask) > 0:
            current = df.loc[mask, attr].to_numpy()
            df.loc[mask, attr] = _sample_different(current, values)

    return df, len(modify_indices)
