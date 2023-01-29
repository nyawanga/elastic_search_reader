"""ELASTIC SEARCH READER MODULE"""
# pylint: disable=no-member,too-many-locals,broad-except,too-few-public-methods,import-error,wrong-import-order,arguments-differ
# type: ignore

from typing import List, Dict, Any, Generator, Union
from elasticsearch import Elasticsearch

from base_readers import BaseReader
from elastic_search_sdk import (
    ParamsHandlerFactory,
    PaginatorFactory,
    PaginatorEngine,
    BasicAuthAuthenticator,
)
from api_utilities.file_managers import load_file
from api_utilities.date_managers import date_range_iterator


class ElasticSearchReader(BaseReader):
    """Reader Class for Elastic Search"""

    def __init__(self, creds_filepath: str, config_filepath: str):
        self.configs = load_file(config_filepath)
        self.secrets = load_file(creds_filepath)
        self.service: Union[Elasticsearch, None] = None
        self.success: List[bool] = []

    def _get_auth(self, authenticator: BasicAuthAuthenticator):
        service = authenticator.authenticate(secrets=self.secrets)
        return service

    def nomalize(self, data: Dict[Any, Any], data_field: str) -> Dict[Any, Any]:
        """Method to Flatten the Data

        Args:
            data (Dict[Any, Any]): results from the api call dictionary

        Returns:
            Dict[Any, Any]: flattened data
        """

        flattened = data.pop(data_field)
        flattened.update({"metadata": data})
        return flattened

    def _query_handler(
        self, paginator_engine: PaginatorEngine, search_params: Dict[str, Any]
    ) -> Generator[Dict[Any, Any], None, None]:
        """method that makes the scan call to elastic search

        Args:
            search_params (Dict[str, Any]): built params for search api call

        Returns:
            Iterable[Dict[Any, Any]]: iterable of results
        """
        iter_response = paginator_engine.paginate(
            service=self.service, search_params=search_params, configs=self.configs
        )
        for record in iter_response:
            yield record

    def run_query(self):
        """Main exit method of the reader"""

        authenticator = BasicAuthAuthenticator()
        self.service = self._get_auth(authenticator=authenticator)

        paginator_engine = PaginatorFactory().get_paginator(
            paginator=self.configs["paginator"]
        )
        params_handler = ParamsHandlerFactory().get_handler(
            syntax=self.configs["syntax"]
        )
        for startdate, _ in date_range_iterator(
            start_date=self.configs.get("start_date", "today"),
            end_date=self.configs.get("end_date", "today"),
            interval=self.configs.get("interval", "day"),
            end_inclusive=True,
            time_format="%Y-%m-%d",
        ):
            dataset: List[Dict[Any, Any]] = []
            search_params = params_handler.build_params(configs=self.configs)

            response = self._query_handler(
                paginator_engine=paginator_engine, search_params=search_params
            )

            dataset = [
                self.nomalize(data_field=self.configs["data_field"], data=record)
                for record in response
            ]
            if not dataset:
                print("no data for provided configs")
                self.not_success()
                continue

            self.is_success()
            yield {
                "date": startdate.replace("-", ""),
                "index": self.configs["index"][0],
                "data": dataset,
            }
