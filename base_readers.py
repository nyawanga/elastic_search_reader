"""BASE READER FOR ALL READERS"""

from abc import ABC, abstractmethod
from typing import Union, Generator, Dict, List, Any
import requests
from requests.adapters import HTTPAdapter, Retry


class Authenticator(ABC):
    """Base Authenticaticator class"""

    @abstractmethod
    def authenticate(self, secrets: Dict[Any, Any]):
        """Authenticator method

        Args:
            secrets (Dict[Any, Any]): _description_

        Raises:
            NotImplementedError: _description_

        Returns:
            _type_: _description_
        """
        raise NotImplementedError


class BaseReader(ABC):
    """Base Reader Class Guiding All Readers"""

    success: List[bool] = []

    @abstractmethod
    def _get_auth(self, **kwargs):
        """Get Authorization"""
        raise NotImplementedError

    @abstractmethod
    def _query_handler(self, **kwargs):
        """Handles the Query Call"""
        raise NotImplementedError

    @abstractmethod
    def run_query(
        self,
    ) -> Union[
        Generator[Dict[List[Dict[Any, Any]], Any], None, None],
        Dict[List[Dict[Any, Any]], Any],
    ]:
        """Calls the _query_handler method and handles iteration"""
        raise NotImplementedError

    def is_success(self):
        """Records the success status of the api call"""
        self.success.append(True)

    def not_success(self):
        """Records the failed status of the API call"""
        self.success.append(False)


class APIRequestsSessionsReader(BaseReader):
    """Base Requests Sessions
    https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry
    """

    sessions: requests.Session = requests.Session()
    session_config: Dict[str, Union[int, bool, str, float]]

    def get_session(self):
        """Builds sessions with Retry Backed In"""
        retries = Retry(
            total=self.session_config.get("total", 3),
            backoff_factor=self.session_config.get("backoff_factor", 0.1),
            status_forcelist=self.session_config.get(
                "status_forcelist", [500, 502, 503, 504]
            ),
            read=self.session_config.get("read", 3),
            redirect=self.session_config.get("redirect", 3),
            raise_on_redirect=self.session_config.get("raise_on_redirect", True),
            raise_on_status=self.session_config.get("raise_on_status", True),
        )
        for protocol in ["http://", "https://"]:
            self.sessions.mount(protocol, HTTPAdapter(max_retries=retries))

        return self.sessions
