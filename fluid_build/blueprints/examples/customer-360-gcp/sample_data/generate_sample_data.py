# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generate Sample Data for Customer 360 GCP
# This script creates realistic sample data for testing the Customer 360 analytics platform

import csv
import random
from datetime import datetime, timedelta

from faker import Faker

from fluid_build.cli.console import cprint

fake = Faker()
Faker.seed(42)  # For reproducible data


def generate_customers(num_customers=100):
    """Generate sample customer data"""
    customers = []

    for i in range(1, num_customers + 1):
        customer_id = f"CUST{i:03d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"

        # Customer since date (within last 2 years)
        customer_since = fake.date_between(start_date="-2y", end_date="today")

        customer = {
            "customer_id": customer_id,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "phone": fake.phone_number(),
            "address_line_1": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "country": "US",
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "gender": random.choice(["Male", "Female", "Non-Binary"]),
            "customer_since": customer_since,
            "customer_status": random.choices(["Active", "Inactive"], weights=[85, 15])[0],
            "marketing_opt_in": random.choice([True, False]),
            "created_at": customer_since.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": customer_since.strftime("%Y-%m-%d %H:%M:%S"),
        }
        customers.append(customer)

    return customers


def generate_transactions(customers, avg_transactions_per_customer=5):
    """Generate sample transaction data"""
    transactions = []
    transaction_id = 1
    order_id = 1

    product_categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Health & Beauty"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay"]
    channels = ["Online", "In-Store", "Mobile App", "Phone"]

    for customer in customers:
        customer_id = customer["customer_id"]
        customer_since = datetime.strptime(customer["customer_since"], "%Y-%m-%d")

        # Number of transactions for this customer (varying by engagement)
        if customer["customer_status"] == "Active":
            num_transactions = random.randint(1, avg_transactions_per_customer * 2)
        else:
            num_transactions = random.randint(0, 2)

        for _ in range(num_transactions):
            # Transaction date after customer_since
            days_since_signup = (datetime.now() - customer_since).days
            if days_since_signup > 0:
                transaction_date = customer_since + timedelta(
                    days=random.randint(1, days_since_signup)
                )
            else:
                transaction_date = customer_since

            # Transaction details
            category = random.choice(product_categories)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10, 200), 2)
            amount = round(quantity * unit_price, 2)

            transaction = {
                "transaction_id": f"TXN{transaction_id:03d}",
                "customer_id": customer_id,
                "order_id": f"ORD{order_id:03d}",
                "transaction_date": transaction_date.strftime("%Y-%m-%d"),
                "transaction_timestamp": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
                "amount": amount,
                "currency": "USD",
                "payment_method": random.choice(payment_methods),
                "transaction_type": "Purchase",
                "status": random.choices(["Completed", "Pending", "Failed"], weights=[90, 5, 5])[0],
                "product_category": category,
                "product_id": f"PROD{random.randint(1, 1000):03d}",
                "quantity": quantity,
                "unit_price": unit_price,
                "channel": random.choice(channels),
                "created_at": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
            }

            transactions.append(transaction)
            transaction_id += 1
            order_id += 1

    return transactions


def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file"""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    cprint(f"Generated {len(data)} records in {filename}")


if __name__ == "__main__":
    cprint("Generating sample data for Customer 360 GCP...")

    # Generate customers
    customers = generate_customers(100)
    customer_fieldnames = [
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "phone",
        "address_line_1",
        "city",
        "state",
        "zip_code",
        "country",
        "date_of_birth",
        "gender",
        "customer_since",
        "customer_status",
        "marketing_opt_in",
        "created_at",
        "updated_at",
    ]
    save_to_csv(customers, "customers_full.csv", customer_fieldnames)

    # Generate transactions
    transactions = generate_transactions(customers, avg_transactions_per_customer=8)
    transaction_fieldnames = [
        "transaction_id",
        "customer_id",
        "order_id",
        "transaction_date",
        "transaction_timestamp",
        "amount",
        "currency",
        "payment_method",
        "transaction_type",
        "status",
        "product_category",
        "product_id",
        "quantity",
        "unit_price",
        "channel",
        "created_at",
        "updated_at",
    ]
    save_to_csv(transactions, "transactions_full.csv", transaction_fieldnames)

    cprint("Sample data generation completed!")
    cprint(f"- Customers: {len(customers)}")
    cprint(f"- Transactions: {len(transactions)}")
    cprint(f"- Average transactions per customer: {len(transactions) / len(customers):.1f}")
