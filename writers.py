"""ELASTIC SEARCH WRITERS MODULE"""

from datetime import datetime

# pylint: disable=too-few-public-methods,import-error
from typing import Dict, Any, Tuple, List, Union
from base_writers import BaseWriter


class ElasticSearchWriter(BaseWriter):
    """Writer class for elastic search data

    Args:
        BaseWriter (_type_): inherits from BaseWriter
    """

    def __init__(
        self, bucket: str, folder_path: str, destination: str, configs: Dict[str, Any]
    ):
        self.success: List[bool] = []
        super().__init__(
            bucket=bucket,
            folder_path=folder_path,
            destination=destination,
            configs=configs,
        )

    def verify_data(
        self, payload: Dict[str, Union[List[Dict[Any, Any]], Any]]
    ) -> Tuple[str, List[Dict[Any, Any]]]:
        """Verifies data meets expected condition and creates write path

        Args:
            payload (Dict[str, Union[List[Dict[Any, Any]], Any]]): payload object

        Returns:
            Tuple[str, List[Dict[Any, Any]]]: full write_path and actual data object
        """
        # confirm the payload keys are matching accurately with what is expected
        if not {"data", "date", "index"}.issubset(set(payload.keys())):
            raise KeyError("invalid payload expecting: data, date, index")

        if not isinstance(payload["data"], list):
            raise TypeError("invalid data passed: expected List[Dict[Any,Any]]")

        try:
            datetime.strptime(payload["date"], "%Y%m%d")
        except ValueError as exc:
            raise ValueError(
                f"wrong date value expected format YYYYMMDD: {payload['index']}"
            ) from exc

        if not isinstance(payload["index"], str):
            raise TypeError(f"wrong 'index' value passed: {payload['index']}")

        data = payload["data"]
        date = payload["date"]
        index = payload["index"]
        write_path = f"{self.folder_path}/{index}/{date}"

        return write_path, data
