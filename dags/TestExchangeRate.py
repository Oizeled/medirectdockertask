import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
from airflow.models import DagBag
from airflow.operators.python_operator import PythonOperator
from ExchangeRate import fetch_and_store_belgium_holidays, fetch_and_store_usd_base_exchange_rate, fetch_and_store_eur_base_exchange_rate
import requests
import psycopg2
import json

class TestExchangeRateDAG(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_fetch_and_store_belgium_holidays(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "startDate": "2023-01-01",
                "name": [{"text": "New Year"}]
            },
            {
                "id": 2,
                "startDate": "2023-12-25",
                "name": [{"text": "Christmas"}]
            }
        ]
        requests.get = MagicMock(return_value=mock_response)
        psycopg2.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor

        fetch_and_store_belgium_holidays()

        mock_conn.commit.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()

    def test_fetch_and_store_usd_base_exchange_rate(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {
                "EUR": 0.9,
                "GBP": 0.7
            }
        }
        requests.get = MagicMock(return_value=mock_response)
        psycopg2.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor

        fetch_and_store_usd_base_exchange_rate()

        mock_conn.commit.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()

    def test_fetch_and_store_eur_base_exchange_rate(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_fetchone_result = (
            {
                "rates": {
                    "EUR": 0.9,
                    "GBP": 0.7
                }
            },
        )
        psycopg2.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = mock_fetchone_result

        fetch_and_store_eur_base_exchange_rate()

        mock_conn.commit.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()

    def test_dag_structure(self):
        dag = self.dagbag.get_dag(dag_id='ExchangeRate_DAG')
        self.assertIsNotNone(dag)

        tasks = dag.tasks
        self.assertEqual(len(tasks), 3)

        task_ids = [task.task_id for task in tasks]
        expected_task_ids = [
            'fetch_data_belgium_holidays',
            'fetch_data_usd_base_exchange_rate',
            'fetch_data_eur_base_exchange_rate'
        ]
        self.assertListEqual(task_ids, expected_task_ids)


if __name__ == '__main__':
    unittest.main()
