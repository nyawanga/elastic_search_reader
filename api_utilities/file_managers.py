"""MODULE TO HELP WITH PROCESSING FILES"""
# pylint: disable=unused-argument
from typing import Union, Dict, Any, List
from pathlib import Path
import json
import yaml


def load_file(file_location: str, fmt: Union[str, None] = None) -> Dict[Any, Any]:
    """
    Gathers file data from json or yaml.
    """
    config: dict = {}
    if file_location.strip().rsplit(".", maxsplit=1)[-1] not in ["json", "yml", "yaml"]:
        raise TypeError("Wrong file type provided! Expecting only json and yaml files")

    file_location = str(file_location).strip()
    if file_location.endswith("yml"):
        with open(file_location, mode="r", encoding="utf8") as yaml_file:
            config = yaml.safe_load(yaml_file)
    if file_location.endswith("json"):
        with open(file_location, mode="r", encoding="utf8") as json_file:
            config = json.load(json_file)

    return config


def local_json_writer(
    payload: Union[Dict[Any, Any], List[Dict[Any, Any]]],
    filename: str,
    root_dir: str,
    folder_path: Union[str, None] = None,
    mode: str = "w",
) -> None:
    """WRITE DATA TO JSON OBJECT"""

    file_path = root_dir
    if folder_path:
        file_path = f"{root_dir}/{folder_path}"
        # CREATE FILE PATH IF NOT EXISTS
    Path(file_path).mkdir(parents=True, exist_ok=True)

    if filename is None or str(filename).strip() == "":
        raise ValueError("please proivde a destination filename")

    filename = f"{file_path}/{filename}.json"

    with open(f"{filename}", mode=mode, encoding="utf8") as dest_file:
        json.dump(payload, dest_file, indent=4)
        print(f"Done writting data to {filename}")
