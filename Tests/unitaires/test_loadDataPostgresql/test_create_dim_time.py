from unittest.mock import MagicMock, patch, call
from Jobs.loadDataPostgresql import create_dim_time


@patch("Jobs.loadDataPostgresql.year")
@patch("Jobs.loadDataPostgresql.month")
@patch("Jobs.loadDataPostgresql.dayofmonth")
@patch("Jobs.loadDataPostgresql.hour")
@patch("Jobs.loadDataPostgresql.minute")
@patch("Jobs.loadDataPostgresql.second")
def test_create_dim_time_basic_flow(
    mock_second, mock_minute, mock_hour, mock_dayofmonth, mock_month, mock_year
):
    # Préparer mocks pour fonctions Spark SQL
    mock_year.return_value = "mock_year_col"
    mock_month.return_value = "mock_month_col"
    mock_dayofmonth.return_value = "mock_day_col"
    mock_hour.return_value = "mock_hour_col"
    mock_minute.return_value = "mock_minute_col"
    mock_second.return_value = "mock_second_col"

    df_mock = MagicMock()
    df_selected = MagicMock()
    df_deduped = MagicMock()

    df_mock.select.return_value = df_selected
    df_selected.dropDuplicates.return_value = df_deduped

    df_deduped.withColumn.return_value = df_deduped

    result = create_dim_time(df_mock)

    df_mock.select.assert_called_once_with("event_ts")
    df_selected.dropDuplicates.assert_called_once_with()

    expected_calls = [
        call("year", "mock_year_col"),
        call("month", "mock_month_col"),
        call("day", "mock_day_col"),
        call("hour", "mock_hour_col"),
        call("minute", "mock_minute_col"),
        call("second", "mock_second_col"),
    ]
    df_deduped.withColumn.assert_has_calls(expected_calls, any_order=False)

    assert result == df_deduped

def test_create_dim_time_with_no_event_ts_column():
    # Scénario où select lève une erreur (par ex colonne manquante)
    df_mock = MagicMock()
    df_mock.select.side_effect = Exception("Column event_ts not found")

    try:
        create_dim_time(df_mock)
        assert False, "Expected Exception was not raised"
    except Exception as e:
        assert str(e) == "Column event_ts not found"

def test_create_dim_time_dropduplicates_returns_none():
    # DropDuplicates peut théoriquement retourner None (exemple extrême)
    df_mock = MagicMock()
    df_selected = MagicMock()

    df_mock.select.return_value = df_selected
    df_selected.dropDuplicates.return_value = None

    try:
        create_dim_time(df_mock)
        assert False, "Expected AttributeError was not raised"
    except AttributeError:
        # because None.withColumn would fail
        pass
