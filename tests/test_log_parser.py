import re
import pytest

from pytest_postgresql import factories

from psycopg import Connection, connect
from regex_log_parser import LogProcessor, HandlerBase, PostgresHandler


def load_database(**kwargs):
    db_connection: Connection = connect(**kwargs)

    with db_connection.cursor() as cur:
        cur.execute(open("tests/test_schema.sql", "r").read())
        db_connection.commit()


postgresql_noproc = factories.postgresql_noproc(
    load=[load_database],
)

postgresql = factories.postgresql(
    "postgresql_noproc",
)


class TestHandler(HandlerBase):
    def startup(self):
        assert True
    
    def shutdown(self):
        assert True

    def handler(self, file_path: str, line: str, match: re.Match):
        assert True


rules = {
    "test.log": {
        "([a-zA-Z0-9]*)": "handler",
    }
}


class MyHandler(PostgresHandler):
    def __init__(self, connection = None, setup_script = None):
        super().__init__(connection=connection, setup_script=setup_script)

    def startup(self):
        assert True

    def shutdown(self):
        assert True

    def handler(self, file_path: str, line: str, match: re.Match):
        name = match.group(1)

        sql = """
            INSERT INTO test (name)
            VALUES
                (%s)
        """
        params = (name,)

        self.queue_op(sql, params, run_now=True)


@pytest.fixture
def postgres_handler(postgresql):
    return MyHandler(
        connection=postgresql,
        setup_script="tests/test_schema.sql",
    )


def test_log_processor_init():
    processor = LogProcessor(
        dry_run=False,
        rules=rules,
        handler=TestHandler(),
    )

    processor.run('tests/logs')


def test_my_handler(postgres_handler):
    processor = LogProcessor(
        dry_run=False,
        rules=rules,
        handler=postgres_handler,
    )

    processor.run('tests/logs')