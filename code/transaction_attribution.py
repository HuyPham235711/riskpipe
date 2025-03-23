from dataclasses import asdict, dataclass, field
from typing import List, Tuple

from jinja2 import Environment, FileSystemLoader
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

REQUIRED_JARS = [
    "file:///opt/flink/flink-sql-connector-kafka-1.17.0.jar",
    "file:///opt/flink/flink-connector-jdbc-3.0.0-1.16.jar",
    "file:///opt/flink/postgresql-42.6.0.jar",
]


@dataclass(frozen=True)
class StreamJobConfig:
    job_name: str = 'transaction-attribution-job'
    jars: List[str] = field(default_factory=lambda: REQUIRED_JARS)
    checkpoint_interval: int = 10
    checkpoint_pause: int = 5
    checkpoint_timeout: int = 5
    parallelism: int = 2


@dataclass(frozen=True)
class KafkaConfig:
    connector: str = 'kafka'
    bootstrap_servers: str = 'kafka:9092'
    scan_stratup_mode: str = 'earliest-offset'
    consumer_group_id: str = 'flink-consumer-group-1'


@dataclass(frozen=True)
class TransactionTopicConfig(KafkaConfig):
    topic: str = 'bank_transactions'
    format: str = 'json'


@dataclass(frozen=True)
class AccountTopicConfig(KafkaConfig):
    topic: str = 'accounts'
    format: str = 'json'
    scan_startup_mode: str = 'earliest-offset'


@dataclass(frozen=True)
class ApplicationDatabaseConfig:
    connector: str = 'jdbc'
    url: str = 'jdbc:postgresql://postgres:5432/postgres'
    username: str = 'postgres'
    password: str = 'postgres'
    driver: str = 'org.postgresql.Driver'


@dataclass(frozen=True)
class ApplicationAccountsTableConfig(ApplicationDatabaseConfig):
    table_name: str = 'banking.accounts'


@dataclass(frozen=True)
class ApplicationAttributedTransactionsTableConfig(ApplicationDatabaseConfig):
    table_name: str = 'banking.attributed_transactions'


def get_execution_environment(config: StreamJobConfig) -> Tuple[StreamExecutionEnvironment, StreamTableEnvironment]:
    s_env = StreamExecutionEnvironment.get_execution_environment()
    for jar in config.jars:
        s_env.add_jars(jar)
    s_env.enable_checkpointing(config.checkpoint_interval * 1000)
    s_env.get_checkpoint_config().set_min_pause_between_checkpoints(config.checkpoint_pause * 1000)
    s_env.get_checkpoint_config().set_checkpoint_timeout(config.checkpoint_timeout * 1000)
    execution_config = s_env.get_config()
    execution_config.set_parallelism(config.parallelism)
    t_env = StreamTableEnvironment.create(s_env)
    job_config = t_env.get_config().get_configuration()
    job_config.set_string("pipeline.name", config.job_name)
    return s_env, t_env


def get_sql_query(
    entity: str, type: str = 'source', template_env: Environment = Environment(loader=FileSystemLoader("code/"))
) -> str:
    config_map = {
        'transactions': TransactionTopicConfig(),
        'accounts': AccountTopicConfig(),
        'attributed_transactions': ApplicationAccountsTableConfig(),
        'attribute_transactions': ApplicationAttributedTransactionsTableConfig(),
    }
    return template_env.get_template(f"{type}/{entity}.sql").render(asdict(config_map.get(entity)))


def run_transaction_attribution_job(t_env: StreamTableEnvironment, get_sql_query=get_sql_query) -> None:
    t_env.execute_sql(get_sql_query('accounts'))
    t_env.execute_sql(get_sql_query('transactions'))
    t_env.execute_sql(get_sql_query('attributed_transactions', 'sink'))
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(get_sql_query('attribute_transactions', 'process'))
    stmt_set.execute()


if __name__ == '__main__':
    _, t_env = get_execution_environment(StreamJobConfig())
    run_transaction_attribution_job(t_env)
