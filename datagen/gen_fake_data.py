import argparse
import json
import random
import time
from datetime import datetime
from uuid import uuid4

import psycopg2
from confluent_kafka import Producer
from faker import Faker
from psycopg2 import OperationalError

fake = Faker()


# Function to create a PostgreSQL connection with retry logic
def get_postgres_connection(retries=10, delay=5):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="postgres", port=5432)
            print("Connected to PostgreSQL!")
            return conn
        except OperationalError as e:
            print(f"Failed connecting to PostgreSQL: {e}. Try again after {delay} seconds... ({attempt+1}/{retries})")
            time.sleep(delay)
    raise Exception("Failed connecting to PostgreSQL after many attemps.")


# Function to push the event to Kafka topic
def push_to_kafka(event, topic, producer):
    producer.produce(topic, json.dumps(event).encode('utf-8'))
    producer.flush()


# Function to generate account data and push to Kafka
def gen_account_data(num_accounts: int, producer):
    account_ids = []
    conn = get_postgres_connection()
    curr = conn.cursor()

    try:
        for _ in range(num_accounts):
            account_id = str(uuid4())
            account_name = fake.name()
            email = fake.email()
            account_type = random.choice(["Personal", "Corporate", "Savings", "Investment"])
            balance = round(random.uniform(1000, 100000), 2)
            created_at = datetime.now()  # Lưu datetime gốc cho PostgreSQL

            # Insert into PostgreSQL
            curr.execute(
                """INSERT INTO banking.accounts
                (account_id, account_name, email, account_type, balance)
                VALUES (%s, %s, %s, %s, %s)""",
                (account_id, account_name, email, account_type, balance),
            )

            # Create account event for Kafka with ISO format (milliseconds)
            account_event = {
                "account_id": account_id,
                "account_name": account_name,
                "email": email,
                "account_type": account_type,
                "balance": balance,
                "created_at": created_at.strftime('%Y-%m-%d %H:%M:%S'),
            }

            # Push account event to Kafka topic 'accounts'
            push_to_kafka(account_event, 'accounts', producer)
            print(f"Generated and filled accounts: {account_id}")
            account_ids.append(account_id)

        conn.commit()
    except Exception as e:
        print(f"Error while inserting to PostgreSQL or push to Kafka: {e}")
        conn.rollback()
    finally:
        curr.close()
        conn.close()

    return account_ids


# Function to generate transaction event and store in PostgreSQL
def generate_transaction_event(account_id, conn):
    curr = conn.cursor()

    transaction_id = str(uuid4())
    transaction_type = random.choice(["Deposit", "Withdrawal", "Transfer"])
    amount = round(random.uniform(10, 500000), 2)
    currency = random.choice(["USD", "EUR", "VND"])
    transaction_status = random.choice(["Pending", "Completed", "Failed"])
    datetime_occured = datetime.now()

    curr.execute(
        """INSERT INTO banking.transactions
        (transaction_id, account_id, transaction_type, amount, currency,
        transaction_status, datetime_occured)
        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (
            transaction_id,
            account_id,
            transaction_type,
            amount,
            currency,
            transaction_status,
            datetime_occured,
        ),
    )

    # Create transaction event for Kafka with ISO format (milliseconds)
    transaction_event = {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "transaction_type": transaction_type,
        "amount": amount,
        "currency": currency,
        "transaction_status": transaction_status,
        "datetime_occured": datetime_occured.strftime('%Y-%m-%d %H:%M:%S'),
    }

    curr.close()
    return transaction_event


# Function to generate transaction stream data continuously
def gen_transaction_stream_data(account_ids, transactions_per_second=1):
    conn = get_postgres_connection()
    producer = Producer({'bootstrap.servers': 'kafka:9092'})

    print("Start streaming generating fake data...")
    try:
        while True:
            account_id = random.choice(account_ids)
            transaction_event = generate_transaction_event(account_id, conn)
            push_to_kafka(transaction_event, 'bank_transactions', producer)
            conn.commit()
            print(f"Generated and filled transactions: {transaction_event['transaction_id']}")
            time.sleep(1 / transactions_per_second)
    except KeyboardInterrupt:
        print("Stop generating on user demand.")
    except Exception as e:
        print(f"Error while generating or pushing to Kafka: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate account and transaction data and push to Kafka continuously')
    parser.add_argument('-na', '--num_accounts', type=int, help='Number of accounts to generate initially', default=100)
    parser.add_argument('-tps', '--transactions_per_second', type=float, help='Number of transactions per second', default=1.0)

    args = parser.parse_args()

    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    account_ids = gen_account_data(args.num_accounts, producer)
    gen_transaction_stream_data(account_ids, args.transactions_per_second)
