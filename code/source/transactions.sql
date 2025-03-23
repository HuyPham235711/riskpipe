CREATE TABLE transactions (
    transaction_id STRING,
    account_id STRING,
    transaction_type STRING,
    amount DECIMAL(15, 2),
    currency STRING,
    transaction_status STRING,
    datetime_occured TIMESTAMP(3) NOT NULL,
    processing_time AS PROCTIME(),
    WATERMARK FOR datetime_occured AS datetime_occured - INTERVAL '15' SECOND
) WITH (
    'connector' = '{{ connector }}',
    'topic' = '{{ topic }}',
    'properties.bootstrap.servers' = '{{ bootstrap_servers }}',
    'properties.group.id' = '{{ consumer_group_id }}',
    'scan.startup.mode' = '{{ scan_stratup_mode }}',
    'format' = '{{ format }}'
);