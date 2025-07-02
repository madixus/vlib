from unittest.mock import MagicMock, patch
from Jobs.aggregateData import aggregate_data

# 🧪 Test 1 – Pipeline complet exécuté avec succès
def test_aggregate_data_success():
    spark = MagicMock()

    fake_avail_df = MagicMock()
    fake_station_df = MagicMock()
    fake_station_latest = MagicMock()
    fake_joined_df = MagicMock()
    fake_joined_df.count.return_value = 123

    with patch("Jobs.aggregateData.read_clean_data", return_value=(fake_avail_df, fake_station_df)) as mock_read, \
         patch("Jobs.aggregateData.clean_columns", return_value=(fake_avail_df, fake_station_df)) as mock_clean, \
         patch("Jobs.aggregateData.select_useful_columns", return_value=(fake_avail_df, fake_station_df)) as mock_select, \
         patch("Jobs.aggregateData.get_latest_station_info", return_value=fake_station_latest) as mock_latest, \
         patch("Jobs.aggregateData.join_and_enrich", return_value=fake_joined_df) as mock_join, \
         patch("Jobs.aggregateData.write_joined_data") as mock_write:

        aggregate_data(spark)

        # Vérifie que tous les appels ont bien eu lieu
        mock_read.assert_called_once_with(spark)
        mock_clean.assert_called_once_with(fake_avail_df, fake_station_df)
        mock_select.assert_called_once_with(fake_avail_df, fake_station_df)
        mock_latest.assert_called_once_with(fake_station_df)
        mock_join.assert_called_once_with(fake_avail_df, fake_station_latest)
        mock_write.assert_called_once_with(fake_joined_df)

        # Vérifie les appels à count() et show()
        fake_joined_df.count.assert_called_once()
        fake_joined_df.show.assert_any_call(5, truncate=False)
        fake_joined_df.show.assert_any_call(20, truncate=False)


# 🧪 Test 2 – Erreur dans une étape intermédiaire
def test_aggregate_data_fails_on_clean_columns():
    spark = MagicMock()
    with patch("Jobs.aggregateData.read_clean_data", return_value=(MagicMock(), MagicMock())), \
         patch("Jobs.aggregateData.clean_columns", side_effect=Exception("Erreur nettoyage")):

        try:
            aggregate_data(spark)
            assert False, "Une exception aurait dû être levée"
        except Exception as e:
            assert str(e) == "Erreur nettoyage"
