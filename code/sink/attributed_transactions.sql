CREATE TABLE attributed_transactions (
    transaction_id STRING,
    account_id STRING,
    account_name STRING,
    transaction_type STRING,
    amount DECIMAL(15, 2),
    currency STRING,
    transaction_status STRING,
    datetime_occured TIMESTAMP,  -- Align with source
    risk_score INT,                -- Match query output
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'banking.attributed_transactions',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
);