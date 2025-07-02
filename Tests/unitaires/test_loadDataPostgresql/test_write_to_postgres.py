from unittest.mock import MagicMock, patch
from Jobs.loadDataPostgresql import write_to_postgres

@patch("Jobs.loadDataPostgresql.logger")
def test_write_to_postgres_success(mock_logger):
    df_mock = MagicMock()
    jdbc_writer = MagicMock()
    df_mock.write = jdbc_writer

    write_to_postgres(df_mock, "jdbc-url", "table_name", {"user": "x", "password": "y"})

    jdbc_writer.jdbc.assert_called_once_with(
        url="jdbc-url",
        table="table_name",
        mode="append",
        properties={"user": "x", "password": "y"}
    )
    mock_logger.info.assert_called_once_with("💾 table_name → PostgreSQL")

@patch("Jobs.loadDataPostgresql.logger")
def test_write_to_postgres_failure(mock_logger):
    df_mock = MagicMock()
    jdbc_writer = MagicMock()
    jdbc_writer.jdbc.side_effect = Exception("connection failed")
    df_mock.write = jdbc_writer

    write_to_postgres(df_mock, "jdbc-url", "table_name", {"user": "x", "password": "y"})

    mock_logger.warning.assert_called_once()
    assert "⚠️ table_name non insérée" in mock_logger.warning.call_args[0][0]

@patch("Jobs.loadDataPostgresql.logger")
def test_write_to_postgres_logs_info(mock_logger):
    df_mock = MagicMock()
    df_mock.write = MagicMock()

    write_to_postgres(df_mock, "jdbc-url", "my_table", {"user": "u", "password": "p"})

    mock_logger.info.assert_called_with("💾 my_table → PostgreSQL")
