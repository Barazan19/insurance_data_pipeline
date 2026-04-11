import random
import csv
from datetime import date, timedelta

# ── Config ──────────────────────────────────────────────────────────────
random.seed(42)

NUM_CUSTOMERS = 1000
NUM_POLICIES  = 3000
CLAIM_RATE    = 0.60   # 60% of policies will have at least one claim

# ── Reference data ──────────────────────────────────────────────────────
MALE_NAMES = [
    "Andi", "Budi", "Dedi", "Eko", "Fajar", "Guntur", "Hendra", "Irwan",
    "Joko", "Kevin", "Lukman", "Made", "Nanda", "Oscar", "Putu", "Rizky",
    "Sandi", "Tegar", "Umar", "Wahyu", "Yusuf", "Zaki", "Arif", "Bayu",
    "Candra", "Doni", "Ferry", "Gilang", "Haris", "Ivan", "Jaka", "Krisna",
    "Luki", "Miko", "Nanang", "Ogi", "Pandu", "Rama", "Surya", "Toni",
    "Rian", "Agus", "Dimas", "Fandi", "Galih", "Hafiz", "Ilham", "Jamal"
]

FEMALE_NAMES = [
    "Siti", "Dewi", "Rina", "Ani", "Bunga", "Citra", "Dian", "Erna",
    "Fitri", "Gina", "Hana", "Indah", "Julia", "Kartika", "Lina", "Maya",
    "Nita", "Okta", "Putri", "Ratna", "Sari", "Tika", "Ulfa", "Vera",
    "Wulan", "Yanti", "Zahira", "Ayu", "Bella", "Clara", "Desi", "Elsa",
    "Fanny", "Grace", "Hesti", "Intan", "Jasmin", "Kirana", "Laras", "Mira",
    "Novia", "Olivia", "Priska", "Reni", "Susan", "Tari", "Unik", "Vina"
]

CITIES = [
    "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
    "Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
    "Bogor", "Malang", "Yogyakarta", "Denpasar", "Balikpapan",
    "Batam", "Pekanbaru", "Banjarmasin", "Pontianak", "Manado"
]

POLICY_TYPES = ["Health", "Life", "Motor", "Property", "Travel"]

CHANNELS = ["Agency", "Bancassurance", "Digital", "Broker", "Direct"]

POLICY_STATUS = ["Active", "Active", "Active", "Lapsed", "Expired"]  # weighted: more Active

CLAIM_STATUS = ["Approved", "Approved", "Rejected", "Pending"]  # weighted: more Approved

DIAGNOSIS_CODES = [f"D{str(i).zfill(3)}" for i in range(1, 51)]

# Premium range per policy type (min, max)
PREMIUM_RANGE = {
    "Health":    (2_000_000, 8_000_000),
    "Life":      (3_000_000, 10_000_000),
    "Motor":     (1_500_000, 5_000_000),
    "Property":  (4_000_000, 15_000_000),
    "Travel":    (500_000,   2_000_000),
}

# Claim amount as % of premium (min, max)
CLAIM_RATIO_RANGE = {
    "Health":    (0.10, 0.90),
    "Life":      (0.50, 1.50),
    "Motor":     (0.20, 1.20),
    "Property":  (0.15, 1.80),
    "Travel":    (0.10, 0.70),
}


# ── Helpers ─────────────────────────────────────────────────────────────
def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def round_to_nearest(value: float, nearest: int = 50_000) -> int:
    return int(round(value / nearest) * nearest)


# ── Generate customers ───────────────────────────────────────────────────
def generate_customers(n: int) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        gender = random.choice(["M", "F"])
        name   = random.choice(MALE_NAMES if gender == "M" else FEMALE_NAMES)
        dob    = random_date(date(1960, 1, 1), date(2000, 12, 31))
        customers.append({
            "customer_id":   f"C{str(i).zfill(4)}",
            "customer_name": name,
            "gender":        gender,
            "birth_date":    dob.isoformat(),
            "city":          random.choice(CITIES),
        })
    return customers


# ── Generate policies ────────────────────────────────────────────────────
def generate_policies(n: int, customer_ids: list[str]) -> list[dict]:
    policies = []
    for i in range(1, n + 1):
        policy_type = random.choice(POLICY_TYPES)
        pmin, pmax  = PREMIUM_RANGE[policy_type]
        premium     = round_to_nearest(random.uniform(pmin, pmax))

        start = random_date(date(2022, 1, 1), date(2024, 6, 30))
        end   = start + timedelta(days=365)

        policies.append({
            "policy_id":      f"P{str(i).zfill(5)}",
            "customer_id":    random.choice(customer_ids),
            "policy_type":    policy_type,
            "premium_amount": premium,
            "start_date":     start.isoformat(),
            "end_date":       end.isoformat(),
            "status":         random.choice(POLICY_STATUS),
            "channel":        random.choice(CHANNELS),
        })
    return policies


# ── Generate claims ──────────────────────────────────────────────────────
def generate_claims(policies: list[dict], claim_rate: float) -> list[dict]:
    claims = []
    claim_counter = 1

    for policy in policies:
        if random.random() > claim_rate:
            continue

        # 1-3 claims per policy
        num_claims = random.choices([1, 2, 3], weights=[0.65, 0.25, 0.10])[0]
        policy_start = date.fromisoformat(policy["start_date"])
        policy_end   = date.fromisoformat(policy["end_date"])
        claim_end    = min(policy_end, date(2024, 12, 31))

        if policy_start >= claim_end:
            continue

        for _ in range(num_claims):
            cmin, cmax   = CLAIM_RATIO_RANGE[policy["policy_type"]]
            claim_amount = round_to_nearest(
                policy["premium_amount"] * random.uniform(cmin, cmax)
            )
            claim_date = random_date(policy_start, claim_end)

            claims.append({
                "claim_id":       f"CL{str(claim_counter).zfill(5)}",
                "policy_id":      policy["policy_id"],
                "claim_date":     claim_date.isoformat(),
                "claim_amount":   claim_amount,
                "claim_status":   random.choice(CLAIM_STATUS),
                "diagnosis_code": random.choice(DIAGNOSIS_CODES),
            })
            claim_counter += 1

    return claims


# ── Write CSV ────────────────────────────────────────────────────────────
def write_csv(filepath: str, rows: list[dict], fieldnames: list[str]):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Written {len(rows):,} rows -> {filepath}")


# ── Main ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Generating Synthetic Insurance Data ===\n")

    customers = generate_customers(NUM_CUSTOMERS)
    write_csv(
        "data/raw/customers.csv",
        customers,
        ["customer_id", "customer_name", "gender", "birth_date", "city"]
    )

    customer_ids = [c["customer_id"] for c in customers]
    policies = generate_policies(NUM_POLICIES, customer_ids)
    write_csv(
        "data/raw/policies.csv",
        policies,
        ["policy_id", "customer_id", "policy_type", "premium_amount",
         "start_date", "end_date", "status", "channel"]
    )

    claims = generate_claims(policies, CLAIM_RATE)
    write_csv(
        "data/raw/claims.csv",
        claims,
        ["claim_id", "policy_id", "claim_date", "claim_amount",
         "claim_status", "diagnosis_code"]
    )

    print(f"\n=== Summary ===")
    print(f"  Customers : {len(customers):,}")
    print(f"  Policies  : {len(policies):,}")
    print(f"  Claims    : {len(claims):,}")
    print(f"\nDone! Run the pipeline to load the new data.")
