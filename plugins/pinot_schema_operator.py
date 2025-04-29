import os
import glob
import json
from typing import Any

import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

class PinotSchemaSubmitOperator(BaseOperator):
    @apply_defaults
    def __init__(self, folder_path, pinot_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context) -> Any:
        try:
            schema_files = glob.glob(self.folder_path + '/*.json')
            for schema_file in schema_files:
                with open(schema_file, 'r') as file:
                    schema_data = json.load(file)  # <-- Parse JSON correctly

                headers = {'Content-Type': 'application/json'}
                response = requests.post(self.pinot_url, headers=headers, json=schema_data)  # <-- Use 'json='

                if response.status_code == 200:
                    self.log.info(f'Schema successfully submitted to Apache Pinot! {schema_file}')
                else:
                    self.log.error(f'Failed to submit schema: {response.status_code} - {response.text}')
                    raise Exception(f'Schema submission failed with status code {response.status_code}')

        except Exception as e:
            self.log.error(f'An error occurred: {str(e)}')
            raise e  # <-- Always re-raise to fail the task properly
