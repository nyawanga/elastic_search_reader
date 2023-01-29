"""TEST MODULE FOR ELASTIC SEARCH READER"""
# pylint: disable=no-member, import-error,wrong-import-position, protected-access
import os
import sys
import pytest
from elasticsearch import Elasticsearch

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from readers import ElasticSearchReader
from elastic_search_sdk import ScrollPaginator
from tests.base_tests import BaseTest


def mock(mocker):
    """mocker function"""
    mocker.patch.object(Elasticsearch, "search")
    mocker.patch.object(Elasticsearch, "open_point_in_time")
    mocker.patch.object(Elasticsearch, "close_point_in_time")
    mocker.patch.object(ScrollPaginator, "paginate")


class TestReader(BaseTest):
    """Class to Test Reader module"""

    dsl_reader_configs = "tests/fixtures/dsl_configs.yml"
    secrets_configs = "tests/fixtures/es_secrets.yml"
    sample_results = "tests/fixtures/sample_results.json"
    normalized_results = "tests/fixtures/normalized_results.json"

    def test_init(self):
        """Test init method"""
        reader = ElasticSearchReader(
            config_filepath="tests/fixtures/sql_configs.yml",
            creds_filepath="tests/fixtures/es_secrets.yml",
        )
        reader_configs = self.load_file(file_location="tests/fixtures/sql_configs.yml")
        secrets_configs = self.load_file(file_location="tests/fixtures/es_secrets.yml")

        assert reader.configs == reader_configs
        assert any(reader.success) is False
        assert reader.service is None
        assert reader.secrets == secrets_configs

    def test_normalize(self):
        """Test normalize method"""
        reader = ElasticSearchReader(
            config_filepath="tests/fixtures/sql_configs.yml",
            creds_filepath="tests/fixtures/es_secrets.yml",
        )

        sample_results = self.load_file(self.sample_results)
        normalized_data = self.load_file(self.normalized_results)
        results = reader.nomalize(data=sample_results[0], data_field="_source")

        assert results == normalized_data

    def test_query_handler(self, mocker):
        """Test query_handler method"""
        mock(mocker)
        reader = ElasticSearchReader(
            config_filepath=self.dsl_reader_configs,
            creds_filepath=self.secrets_configs,
        )

        sample_results = self.load_file(self.sample_results)
        ScrollPaginator.paginate.return_value = sample_results
        scroll_paginator = ScrollPaginator()

        results = reader._query_handler(
            paginator_engine=scroll_paginator, search_params={"test": "test"}
        )

        assert next(results) == sample_results[0]

    def test_run_query_success(self, mocker):
        """Test successful run query method"""
        mock(mocker)
        reader = ElasticSearchReader(
            config_filepath=self.dsl_reader_configs,
            creds_filepath=self.secrets_configs,
        )

        sample_results = self.load_file(self.sample_results)
        normalized_data = self.load_file(self.normalized_results)
        ScrollPaginator.paginate.return_value = sample_results

        results = reader.run_query()
        expected = {
            "date": "20230129",
            "data": [normalized_data],
            "index": "movies",
        }
        assert next(results) == expected
        assert any(reader.success) is True

    def test_run_query_fail(self, mocker):
        """Test failed run query method"""
        mock(mocker)
        reader = ElasticSearchReader(
            config_filepath=self.dsl_reader_configs,
            creds_filepath=self.secrets_configs,
        )

        ScrollPaginator.paginate.return_value = {}
        results = reader.run_query()

        with pytest.raises(StopIteration):
            next(results)
        assert any(reader.success) is False
