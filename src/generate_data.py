from faker import Faker
import pandas as pd
import random

fake = Faker()

def generate_security_id():
    return f"XX{random.randint(1000000000, 9999999999)}"

def generate_transactions(n=500, start_id=0, inject_errors=False):
    rows = []
    for i in range(start_id, start_id + n):
        row = {
            "transaction_id": f"TXN{i:07d}",
            "trade_date": fake.date_between(start_date="-30d", end_date="today"),
            "customer_id": f"CUST{random.randint(1000, 9999)}",
            "account_id": f"ACC{random.randint(100000, 999999)}",
            "security_id": generate_security_id(),
            "amount": round(random.uniform(10, 50000), 2),
            "currency": random.choice(["EUR", "USD", "GBP"]),
            "country": fake.country_code(),
        }
        if inject_errors and random.random() < 0.01:
            row["amount"] = None
        if inject_errors and random.random() < 0.005:
            row["amount"] = -abs(row["amount"] or 100)
        rows.append(row)
    return pd.DataFrame(rows)

if __name__ == "__main__":
    FILE_COUNT = 100
    ROWS_PER_FILE = 50000

    next_id = 0
    for idx in range(1, FILE_COUNT + 1):
        df = generate_transactions(n=ROWS_PER_FILE, start_id=next_id, inject_errors=True)
        df.to_csv(f"data/transactions_{idx:03d}.csv", index=False)
        next_id += ROWS_PER_FILE

    print(f"Creation completed: transactions_001~{FILE_COUNT:03d}.csv ({ROWS_PER_FILE} rows each, {FILE_COUNT * ROWS_PER_FILE} total), each with ~1% missing amount and ~0.5% negative amount")