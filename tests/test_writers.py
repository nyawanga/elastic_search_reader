"""TEST MODULE FOR ELASTIC SEARCH READER"""
# pylint: disable=no-member, import-error,wrong-import-position, protected-access
import os
import sys
import json
import pytest
import boto3

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from writers import ElasticSearchWriter
from tests.base_tests import BaseTest
from base_writers import LocalJSONWriter

# def mock(mocker):
#     """mocker function"""
#     mocker.patch.object(Elasticsearch, "search")
#     mocker.patch.object(Elasticsearch, "open_point_in_time")
#     mocker.patch.object(Elasticsearch, "close_point_in_time")
#     mocker.patch.object(ScrollPaginator, "paginate")


def mock(mocker):
    """Mocker object"""
    mocker.patch.object(boto3, "Session")
    mocker.patch.object(json, "dump")


class TestWriter(BaseTest):
    """Test Class for Writers"""

    def test_writers_init(self, mocker):
        """Test Writers init Function"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search/bucket",
            destination="local_json",
            configs={"profile_name": None},
        )

        assert writer.bucket == "test-bucket"
        assert writer.folder_path == "elastic_search/bucket"
        assert writer.destination == "local_json"
        assert writer.configs == {"profile_name": None}
        assert isinstance(writer.resource, LocalJSONWriter)
        assert any(writer.success) is False

    def test_is_success_update(self, mocker):
        """Test Writers success update"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search/bucket",
            destination="local_json",
            configs={"profile_name": None},
        )
        assert any(writer.success) is False
        writer.is_success()
        assert any(writer.success) is True

    def test_write_data_success(self, mocker):
        """Test Writers successful write"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search/bucket",
            destination="local_json",
            configs={"profile_name": None},
        )
        assert any(writer.success) is False
        writer.write_data(
            payload={
                "data": [{"test": "test_data"}],
                "date": "20230129",
                "index": "movies",
            }
        )
        # confirm it actually goes to the end and uploads the file
        assert any(writer.success) is True

    def test_write_data_invalid_payload_keys(self, mocker):
        """Test Writers has valid key items data, date, dimension"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search/bucket",
            destination="local_json",
            configs={"profile_name": None},
        )
        assert any(writer.success) is False
        # confirm the payload passed from writer actually has
        # the 3 keys used for partitioning data, date, networks
        with pytest.raises(KeyError):
            writer.write_data({"data": [{"test": "test"}]})
        assert any(writer.success) is False

    def test_write_data_valid_data_payload_type(self, mocker):
        """Test Writers data is a list type"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search/bucket",
            destination="local_json",
            configs={"profile_name": None},
        )
        assert any(writer.success) is False
        with pytest.raises(TypeError):
            writer.write_data(
                payload={"data": {}, "date": "20230109", "index": "movies"}
            )
        assert any(writer.success) is False

    def test_write_data_valid_date_partition_value(self, mocker):
        """Test Writers"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search/bucket",
            destination="local_json",
            configs={"profile_name": None},
        )

        with pytest.raises(ValueError):
            writer.write_data(
                {"data": [{"test": "test"}], "date": "2023-01-29", "index": "test"}
            )
        assert any(writer.success) is False

    def test_write_data_valid_index_partition_value(self, mocker):
        """Test Writers index partition value"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search_bucket",
            destination="local_json",
            configs={"profile_name": None},
        )

        with pytest.raises(TypeError):
            writer.write_data(
                {"data": [{"test": "test"}], "date": "20230129", "index": None}
            )
        assert any(writer.success) is False

    def test_verify_data_return_values(self, mocker):
        """Test verify data return value"""
        mock(mocker)
        writer = ElasticSearchWriter(
            bucket="test-bucket",
            folder_path="elastic_search_bucket",
            destination="local_json",
            configs={"profile_name": None},
        )

        write_path, data = writer.verify_data(
            payload={
                "data": [{"test": "test_data"}],
                "date": "20230129",
                "index": "movies",
            }
        )
        assert write_path == "elastic_search_bucket/movies/20230129"
        assert data == [{"test": "test_data"}]
