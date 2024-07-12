"""
## Use setup/ teardown with data quality checks during creation of a Postgres table

This DAG demonstrates a table creation pattern which includes both halting and 
non-halting data quality checks. Setup/ teardown tasks are used to create and
drop temporary tables.

To use this DAG you will need to provide the `postgres_default` connection.
"""

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)

POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "national_parks"
SCHEMA_NAME = "public"


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["setup/teardown", "data quality", "webinar", "use case"],
    default_args={"postgres_conn_id": POSTGRES_CONN_ID, "conn_id": POSTGRES_CONN_ID},
)
def create_table_setup_teardown_postgres():
    @task
    def upstream_task():
        return "hi"

    @task_group
    def create_table():
        create_tmp = PostgresOperator(
            task_id="create_tmp",
            sql=f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}_tmp (
                    park_code varchar(4) PRIMARY KEY,
                    park_name varchar(255),
                    state varchar(255),
                    acres int,
                    latitude float,
                    longitude float
                );""",
        )

        load_data_into_tmp = PostgresOperator(
            task_id="load_data_into_tmp",
            sql=f"""
                    BEGIN;
                    CREATE TEMP TABLE copy_tmp_table 
                    (LIKE {TABLE_NAME}_tmp INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                        
                    COPY copy_tmp_table FROM '/include/parks.csv' 
                    DELIMITER ',' CSV HEADER;
                        
                    INSERT INTO {TABLE_NAME}_tmp
                    SELECT *
                    FROM copy_tmp_table
                    ON CONFLICT DO NOTHING;
                    COMMIT;
                """,
        )

        @task_group
        def test_tmp():
            SQLColumnCheckOperator(
                task_id="test_cols",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}_tmp",
                column_mapping={"acres": {"null_check": {"equal_to": 0}}},
                accept_none="True",
            )

            SQLTableCheckOperator(
                task_id="test_table",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}_tmp",
                checks={"row_count_check": {"check_statement": "COUNT(*) > 30"}},
            )

        swap = PostgresOperator(
            task_id="swap",
            sql=f"""
                ALTER TABLE {TABLE_NAME} RENAME TO {TABLE_NAME}_backup;
                CREATE TABLE {TABLE_NAME} AS SELECT * FROM {TABLE_NAME}_tmp;
                """,
        )

        drop_tmp = PostgresOperator(
            task_id="drop_tmp",
            sql=f"""
                DROP TABLE {TABLE_NAME}_tmp;
                """,
        )

        drop_backup = PostgresOperator(
            task_id="drop_backup",
            sql=f"""
                DROP TABLE {TABLE_NAME}_backup;
                """,
        )

        @task
        def done():
            return "New table is ready!"

        chain(
            create_tmp,
            load_data_into_tmp,
            test_tmp(),
            swap,
            drop_tmp,
            drop_backup,
            done(),
        )

        # define setup/ teardown relationship
        drop_tmp.as_teardown(setups=[create_tmp, load_data_into_tmp])
        drop_backup.as_teardown(setups=[swap])

        @task_group
        def validate():
            test_cols = SQLColumnCheckOperator(
                task_id="test_cols",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}",
                column_mapping={"park_name": {"unique_check": {"equal_to": 0}}},
                accept_none="True",
            )

            test_table = SQLTableCheckOperator(
                task_id="test_table",
                retry_on_failure="True",
                table=f"{SCHEMA_NAME}.{TABLE_NAME}",
                checks={"row_count_check": {"check_statement": "COUNT(*) > 50"}},
            )

            @task(trigger_rule="all_done")
            def sql_check_done():
                return "Additional data quality checks are done!"

            [test_cols, test_table] >> sql_check_done()

        swap >> validate()

    @task
    def downstream_task():
        return "hi"

    upstream_task() >> create_table() >> downstream_task()


create_table_setup_teardown_postgres()
