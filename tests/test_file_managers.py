"""MODULE TO TEST FILE MANAGEERS FROM API UTILITIES"""

# pylint: disable=protected-access, wrong-import-position, import-error, unused-argument
# from pathlib import Path
import os
import sys
import pytest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from tests.base_tests import BaseTest
from api_utilities.file_managers import load_file, local_json_writer


class TestFileManagers(BaseTest):
    """Class to Test File Managers Functions"""

    json_file = "tests/fixtures/sample_results.json"
    yml_file = "tests/fixtures/es_configs.yml"
    yaml_file = "tests/fixtures/test_yaml_file.yaml"
    wrong_file = "tests/fixtures/test_wrong_file_type.txt"

    def test_load_file(self):
        """Test load_file function from file_managers"""
        assert self.load_file(self.json_file) == load_file(self.json_file)
        assert self.load_file(self.yml_file) == load_file(self.yml_file)
        assert self.load_file(self.yaml_file) == load_file(self.yaml_file)

        with pytest.raises(TypeError):
            load_file(self.wrong_file)

    def test_local_json_writer(self):
        """Tests local_json_writer"""

        payload = {"test_write": "sample data"}
        # filename = "local_json_writer"
        root_dir = "test"
        folder_path = "fixtures/tests/fixtures/api_utilities"

        with pytest.raises(ValueError):
            local_json_writer(
                payload=payload,
                filename=None,
                root_dir=root_dir,
                folder_path=folder_path,
                mode="w",
            )
