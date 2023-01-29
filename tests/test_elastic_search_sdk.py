"""TEST MODULE FOR ELASTIC SEARCH SDK"""
# pylint: disable=no-member, import-error,wrong-import-position
import os
import sys
import pytest

from elasticsearch import Elasticsearch
from elasticsearch import helpers

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from tests.base_tests import BaseTest
from elastic_search_sdk import (
    SQLQuery,
    DSLQuery,
    ParamsHandlerFactory,
    ScrollPaginator,
    PointInTimePaginator,
    PaginatorFactory,
)


def mock(mocker):
    """mocker function"""
    mocker.patch.object(Elasticsearch, "search")
    mocker.patch.object(helpers, "scan")
    mocker.patch.object(Elasticsearch, "open_point_in_time")
    mocker.patch.object(Elasticsearch, "close_point_in_time")


class TestSDK(BaseTest):
    """Class to Test ealsticsearch_sdk"""

    def test_sql_query(self):
        """Test SQL Params parser"""
        parser = SQLQuery()
        reader_configs = self.load_file(file_location="tests/fixtures/sql_configs.yml")
        results = parser.build_params(configs=reader_configs)

        assert results["q"] == reader_configs["query"]
        assert results["timeout"] == f"{reader_configs['timeout']}s"

    def test_dsl_query(self):
        """Test DSL Params parser"""
        parser = DSLQuery()
        reader_configs = self.load_file(file_location="tests/fixtures/sql_configs.yml")
        results = parser.build_params(configs=reader_configs)
        assert results["query"] == reader_configs["query"]
        assert results["timeout"] == f"{reader_configs['timeout']}s"

    def test_params_handler_factory(self):
        """Test Params Handler Factory"""
        factory = ParamsHandlerFactory()
        sql_handler = factory.get_handler(syntax="sql")
        dsl_handler = factory.get_handler(syntax="dsl")

        assert isinstance(sql_handler, SQLQuery)
        assert isinstance(dsl_handler, DSLQuery)
        with pytest.raises(KeyError):
            factory.get_handler("invalid_syntax_value")

    def test_paginator_factory(self):
        """Test Paginator Factory"""
        factory = PaginatorFactory()
        scroll_paginator = factory.get_paginator(paginator="scroll")
        pit_paginator = factory.get_paginator(paginator="point_in_time")

        assert isinstance(scroll_paginator, ScrollPaginator)
        assert isinstance(pit_paginator, PointInTimePaginator)
        with pytest.raises(KeyError):
            factory.get_paginator("invalid_paginator_value")

    def test_scroll_paginator(self, mocker):
        """Test Scroll Paginator"""

        mock(mocker)
        paginator = ScrollPaginator()
        expected = self.load_file("tests/fixtures/sample_results.json")
        service = Elasticsearch(
            hosts="https://localhost:9200",
            basic_auth=("test", "test"),
        )
        helpers.scan.return_value = expected
        iter_results = paginator.paginate(
            service=service,
            configs={"test": "test"},
            search_params={"test": "test"},
        )
        results = next(iter_results)

        assert isinstance(results, dict)
        assert results == expected[0]

    def test_point_in_time_paginator(self, mocker):
        """Test Point In Time Paginator"""

        mock(mocker)
        paginator = PointInTimePaginator()
        service = Elasticsearch(
            hosts="https://localhost:9200",
            basic_auth=("test", "test"),
        )
        Elasticsearch.search.open_point_in_time.return_value = {"id": "pointintimeid"}
        Elasticsearch.search.return_value = {"pit_id": "", "hits": {"hits": []}}
        iter_results = paginator.paginate(
            service=service,
            configs={"index": ["test"]},
            search_params={"index": ["test"]},
        )
        with pytest.raises(StopIteration):
            next(iter_results)

    # def test_point_in_time(self, mocker):
    #     """Test Point In Time"""

    #     mock(mocker)
    #     paginator = PointInTimePaginator()
    #     service = Elasticsearch(
    #         hosts="https://localhost:9200",
    #         basic_auth=("test", "test"),
    #     )
    #     # paginator.start_pit.
    #     # Elasticsearch.search.open_point_in_time.return_value = {"id": "pointintimeid"}
    #     # assert isinstance(results, dict)
    #     # pit_id = paginator.start_pit(service=service, index=["test"], keep_alive="5m")
    #     # print(pit_id)
    #     # print(paginator.start_pit(service=service, index=["test"], keep_alive="5m"))
    #     mocker.patch.object(
    #         "Elasticsearch.search.close_point_in_time.side_effect",
    #         Exception(),
    #     )
    #     stop_pit_response = paginator.stop_pit(service=service, pit_id="pointintimeid")
    #     # assert pit_id == "pointintimeid"
    #     assert stop_pit_response is False

    #     # assert results == expected[0]
