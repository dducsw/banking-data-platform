"""Merchant dimension generator.

Builds a static merchants table from the MERCHANTS dict used by
transactions.py. Adds MCC code (ISO 18245), merchant_type, and a stable
merchant_id for joining against transactions.merchant_name.
"""

from typing import Dict

# MCC codes per category (ISO 18245 standard)
# category -> (mcc_code, display_category, merchant_type)
CATEGORY_META: Dict[str, tuple] = {
    "gas_transport":  ("5541", "Gas & Transport",      "physical"),
    "grocery_net":    ("5411", "Grocery",               "online"),
    "grocery_pos":    ("5411", "Grocery",               "physical"),
    "misc_net":       ("5999", "General Merchandise",   "online"),
    "misc_pos":       ("5999", "General Merchandise",   "physical"),
    "shopping_net":   ("5945", "Shopping",              "online"),
    "shopping_pos":   ("5311", "Shopping",              "physical"),
    "entertainment":  ("7922", "Entertainment",         "online"),
    "food_dining":    ("5812", "Food & Dining",         "physical"),
    "health_fitness": ("5912", "Health & Fitness",      "physical"),
    "home":           ("5251", "Home Improvement",      "physical"),
    "kids_pets":      ("5995", "Kids & Pets",           "physical"),
    "personal_care":  ("7230", "Personal Care",         "physical"),
    "travel":         ("4511", "Travel",                "physical"),
}

# Map merchant names to their categories (from MERCHANTS dict in transactions.py)
# Extended with extra metadata
MERCHANT_CATALOG = [
    # gas_transport
    ("Shell",               "gas_transport"),
    ("Exxon",              "gas_transport"),
    ("Uber",               "gas_transport"),
    ("Lyft",               "gas_transport"),
    # grocery_net
    ("Instacart",          "grocery_net"),
    ("Amazon Fresh",       "grocery_net"),
    ("Walmart Online",     "grocery_net"),
    # grocery_pos
    ("Walmart",            "grocery_pos"),
    ("Kroger",             "grocery_pos"),
    ("Safeway",            "grocery_pos"),
    ("Trader Joe's",       "grocery_pos"),
    # misc_net
    ("Amazon",             "misc_net"),
    ("eBay",               "misc_net"),
    # misc_pos
    ("Target",             "misc_pos"),
    ("Dollar Tree",        "misc_pos"),
    # shopping_net
    ("Best Buy Online",    "shopping_net"),
    # shopping_pos
    ("Macy's",             "shopping_pos"),
    ("Home Depot",         "home"),       # primary: home
    ("Best Buy",           "shopping_pos"),
    # entertainment
    ("Netflix",            "entertainment"),
    ("Spotify",            "entertainment"),
    ("AMC Theatres",       "entertainment"),
    ("Disney+",            "entertainment"),
    ("Hulu",               "entertainment"),
    # food_dining
    ("McDonald's",         "food_dining"),
    ("Starbucks",          "food_dining"),
    ("Chipotle",           "food_dining"),
    ("Chick-fil-A",        "food_dining"),
    ("DoorDash",           "food_dining"),
    ("Uber Eats",          "food_dining"),
    # health_fitness
    ("CVS",                "health_fitness"),
    ("Walgreens",          "health_fitness"),
    ("Planet Fitness",     "health_fitness"),
    ("Equinox",            "health_fitness"),
    # home
    ("Lowe's",             "home"),
    ("IKEA",               "home"),
    ("Wayfair",            "home"),
    # kids_pets
    ("Petco",              "kids_pets"),
    ("PetSmart",           "kids_pets"),
    ("Carter's",           "kids_pets"),
    # personal_care
    ("Sephora",            "personal_care"),
    ("Ulta",               "personal_care"),
    ("Supercuts",          "personal_care"),
    # travel
    ("Delta Airlines",     "travel"),
    ("United Airlines",    "travel"),
    ("Expedia",            "travel"),
    ("Airbnb",             "travel"),
    ("Marriott",           "travel"),
    # special / system
    ("Employer Corp",      None),   # salary — not in MERCHANTS, handled separately
    ("Bank Loan Dept",     None),   # loan EMI
    ("Bank Card Division", None),   # credit card payment
]

# Merchant type override for known online-only brands
_ONLINE_ONLY = {"Netflix", "Spotify", "Disney+", "Hulu", "Instacart",
                "Amazon Fresh", "Walmart Online", "eBay", "Amazon",
                "Best Buy Online", "DoorDash", "Uber Eats", "Airbnb",
                "Wayfair", "Expedia"}


def generate_merchant_master() -> list[dict]:
    """Build the merchants reference table.

    merchant_id is a stable integer derived from position in MERCHANT_CATALOG.
    The mapping merchant_name -> merchant_id should be used when writing
    transactions to populate merchant_id (denormalized join key).

    Returns list of merchants dicts.
    """
    rows = []
    for idx, (name, category) in enumerate(MERCHANT_CATALOG, start=1):
        if category is None:
            # System/internal merchants — no MCC
            rows.append({
                "merchant_id": idx,
                "merchant_name": name,
                "merchant_category": "Internal",
                "mcc_code": "9999",
                "merchant_type": "internal",
                "is_online": False,
            })
        else:
            mcc, display_cat, m_type = CATEGORY_META[category]
            rows.append({
                "merchant_id": idx,
                "merchant_name": name,
                "merchant_category": display_cat,
                "mcc_code": mcc,
                "merchant_type": "online" if name in _ONLINE_ONLY else m_type,
                "is_online": name in _ONLINE_ONLY,
            })
    return rows


def build_merchant_lookup() -> Dict[str, int]:
    """Return {merchant_name: merchant_id} lookup for use in transaction generation."""
    return {row["merchant_name"]: row["merchant_id"] for row in generate_merchant_master()}
