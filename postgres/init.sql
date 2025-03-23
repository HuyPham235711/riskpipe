-- create a commerce schema
CREATE SCHEMA banking;

-- Use commerce schema
SET
    search_path TO banking;

-- create a table named accounts
CREATE TABLE accounts (
    account_id UUID PRIMARY KEY,
    account_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    account_type VARCHAR(255) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL
);

-- create a table named transactions
CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    account_id UUID REFERENCES accounts(account_id),
    transaction_type VARCHAR(50) NOT NULL,
    amount NUMERIC(15, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    transaction_status VARCHAR(50) NOT NULL,
    datetime_occured TIMESTAMP NOT NULL
);

ALTER TABLE
    accounts REPLICA IDENTITY FULL;

ALTER TABLE
    transactions REPLICA IDENTITY FULL;

-- create a table named attributed_transactions
CREATE TABLE attributed_transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255),
    account_name VARCHAR,
    transaction_type VARCHAR,
    amount DECIMAL(15, 2),
    currency VARCHAR,
    transaction_status VARCHAR,
    datetime_occured TIMESTAMP,
    risk_score DECIMAL(5, 2)
);