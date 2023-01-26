"""ELASTIC SEARCH SDK"""
# pylint: disable=too-few-public-methods, broad-except
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Generator, Iterable
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class BasicAuthAuthenticator:
    """Basic Authenticator Class For Elastic Search"""

    def authenticate(self, secrets: Dict[str, Any]) -> Elasticsearch:
        """Authenticates elastic search using username and password

        Args:
            secrets (Dict[str, Any]): expects three values i.e
                                     - host (str) node url of host
                                     - port (int) port to access it with
                                     - username (str)
                                     - password (str)

        Returns:
            Elasticsearch: returns instance of Elasticsearch client
        """
        service = Elasticsearch(
            f"{secrets['host']}:{secrets['port']}",
            basic_auth=(secrets["username"], secrets["password"]),
        )
        return service


class ParamsHandler(ABC):
    """
    Base Query Handler Class
    https://elasticsearch-py.readthedocs.io/en/v8.6.0/api.html#module-elasticsearch
    """

    @abstractmethod
    def build_params(self, configs: Dict[Any, Any], **kwargs):
        """Abstract Method for Running Query"""
        raise NotImplementedError


class SQLQuery(ParamsHandler):
    """
    class to run query using SQL statements

    https://www.elastic.co/guide/en/elasticsearch/reference/8.6/sql-search-api.html
    https://elasticsearch-py.readthedocs.io/en/v8.6.0/api.html#module-elasticsearch
    """

    def build_params(self, configs: Dict[Any, Any], **kwargs):
        """Builds Params for SQL Query"""
        params = {
            "index": configs["index"],
            "size": configs.get("size", 10000),
            "q": configs["query"],
            "timeout": f"{configs.get('timeout', 60)}s",
            "batched_reduce_size": configs.get("batched_reduce_size", 20),
            "min_score": configs.get("min_score"),
        }
        return params


class DSLQuery(ParamsHandler):
    """
    class to run query using DSL queries

    https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl.html
    https://elasticsearch-py.readthedocs.io/en/v8.6.0/api.html#module-elasticsearch
    """

    def build_params(self, configs: Dict[Any, Any], **kwargs):
        """Builds Params for DSL Query"""
        params = {
            "index": configs["index"],
            "size": configs.get("size", 10000),
            "query": configs["query"],
            "timeout": f"{configs.get('timeout', 60)}s",
            "batched_reduce_size": configs.get("batched_reduce_size", 20),
            "min_score": configs.get("min_score"),
        }
        return params


class ParamsHandlerFactory:
    """class to get the method for processing params based on syntax we use"""

    def get_handler(self, syntax: str) -> ParamsHandler:
        """Gets params based on the syntax

        Args:
            syntax (str): expects one of two values ["sql", "dsl"]

        Returns:
            ParamsHandler: instance of ParamsHandler
        """
        param_handlers = {"sql": SQLQuery, "dsl": DSLQuery}
        if syntax not in param_handlers:
            raise KeyError(
                f"wrong value for 'syntax' in configs expecting 'sql' or 'dsl' we got {syntax}"
            )

        return param_handlers[syntax]()


class PaginatorEngine(ABC):
    """Pagination Interface Class"""

    def paginate(
        self,
        service: Elasticsearch,
        configs: Dict[Any, Any],
        search_params: Dict[Any, Any],
    ) -> Generator[Dict[Any, Any], None, None]:
        """Paginator for records
        Args:
            configs (Dict[Any,Any]): _description_
            search_params (Dict[Any,Any]): _description_

        Returns:
            Iterable[Dict[Any,Any]]: iterable with the results
        """
        raise NotImplementedError


class ScrollPaginator(PaginatorEngine):
    """Pagination using scroll endpoint"""

    def paginate(
        self,
        service: Elasticsearch,
        configs: Dict[Any, Any],
        search_params: Dict[Any, Any],
    ) -> Generator[Dict[Any, Any], None, None]:
        """Paginator for records
        Args:
            configs (Dict[Any,Any]): _description_
            search_params (Dict[Any,Any]): _description_

        Returns:
            Iterable[Dict[Any,Any]]: iterable with the results
        """
        iter_response: Iterable[Dict[Any, Any]] = helpers.scan(
            service,
            query=search_params,
            scroll=configs.get("scroll", "5m"),
            raise_on_error=True,
            preserve_order=False,
            size=configs.get("size", 10000),
            request_timeout=configs.get("timeout", 60),
            clear_scroll=True,
            scroll_kwargs=None,
        )
        for record in iter_response:
            yield record


class PointInTimePaginator(PaginatorEngine):
    """Point In Time Pagination class"""

    def stop_pit(self, service: Elasticsearch, pit_id: str) -> bool:
        """close the point in time used for pagination

        Args:
            service (Elasticsearch): instance of Elasticsearch
            pit_id (str): string for point in time id

        Returns:
            bool: True if successful or False otherwise
        """
        try:
            service.close_point_in_time(id=pit_id)
            return True
        except Exception as exc:
            print(exc)
            return False

    def start_pit(
        self, service: Elasticsearch, index: List[str], keep_alive: str
    ) -> str:
        """Start a Point in Time to be Used to Paginate on the data

        Args:
            index (List[str]) – A comma-separated list of index names to open point in time;
                            use _all or empty string to perform the operation on all indices
            keep_alive (str) – Specific the time to live for the point in time

        Returns:
            pit_id (str): id for point in time used in search request
        """
        pit = service.open_point_in_time(
            index=index,
            keep_alive=keep_alive,
            ignore_unavailable=True,
        )
        return pit["id"]

    def paginate(
        self,
        service: Elasticsearch,
        configs: Dict[Any, Any],
        search_params: Dict[Any, Any],
    ) -> Generator[Dict[Any, Any], None, None]:

        index: List[str] = configs["index"]
        keep_alive: str = f"{configs.get('keep_alive', 5)}m"
        pit_id = self.start_pit(service=service, index=index, keep_alive=keep_alive)
        counter: int = 0
        start_index: List[Any] = []
        del search_params["index"]  # point in time request does not work with indices
        while True:
            if counter > 0 and configs.get("sort"):
                # print(start_index)
                # you can only add search_after parameter after first call
                search_params["search_after"] = start_index
            response = service.search(
                pit={
                    "id": pit_id,
                    "keep_alive": keep_alive,
                },
                track_total_hits=False,
                **search_params,
                sort=configs.get("sort"),
            )
            records: List[Dict[Any, Any]] = response.get("hits").get("hits")
            pit_id = response["pit_id"]
            if not records:
                self.stop_pit(service=service, pit_id=pit_id)
                break

            for record in records:
                yield record
            start_index = records[-1]["sort"]
            counter += 1


class PaginatorFactory:
    """
    Class to get factory
    """

    def get_paginator(self, paginator: str) -> PaginatorEngine:
        """class method to return paginator engine

        Args:
            paginator (str): string ('scroll', 'point_in_time')

        Raises:
            KeyError: error if the value for paginator is wrong

        Returns:
            PaginatorEngine: paginator engine to use
        """
        paginators = {"scroll": ScrollPaginator, "point_in_time": PointInTimePaginator}
        if paginator not in paginators:
            raise KeyError(f"wrong value paginator {paginator}")

        return paginators[paginator]()
