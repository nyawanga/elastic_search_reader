"""ELASTIC SEARCH WRITERS MODULE"""

# pylint: disable=too-few-public-methods,import-error
from typing import Dict, Any, Tuple, List, Union
from base_writers import BaseWriter


class ElasticSearchWriter(BaseWriter):
    """Writer class for elastice search data

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

        data = payload["data"]
        date = payload["date"]
        index = payload["index"]
        write_path = f"{self.folder_path}/{index}/{date}"

        return write_path, data
