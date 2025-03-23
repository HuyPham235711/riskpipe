CREATE TEMPORARY TABLE accounts (
    account_id STRING,
    account_name STRING,
    email STRING,
    account_type STRING,
    balance DECIMAL(15, 2),
    processing_time AS PROCTIME()
) WITH (
    'connector' = '{{ connector }}',
    'topic' = '{{ topic }}',
    'properties.bootstrap.servers' = '{{ bootstrap_servers }}',
    'properties.group.id' = '{{ consumer_group_id }}',
    'scan.startup.mode' = '{{ scan_startup_mode }}',
    'value.format' = '{{ format }}'
);