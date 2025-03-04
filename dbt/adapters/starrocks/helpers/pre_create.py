import dataclasses
import os
import re
from typing import Optional

import dbt


PRE_CREATE_CONFIG_TAG = "+pre_create"
PRE_CREATE_INSERT_COLUMNS_TAG = "insert_columns"
PRE_CREATE_MODEL_DIR = "pre_create"
PRE_CREATE_TEMPLATE_PREFIX = "template_"


@dataclasses.dataclass
class PreCreateSQLHandler:
    raw_sql_statement: str
    create_statement: Optional[str] = None
    config_statement: Optional[str] = None
    insert_statement: Optional[str] = None
    db_name: Optional[str] = None
    table_name: Optional[str] = None

    @property
    def model_name(self) -> str:
        return self.table_name.split("__dbt")[0]

    @staticmethod
    def _get_relations_from_sql(sql: str) -> tuple[str, str]:
        """

        :param sql:
        :return:
        """
        # Remove leading whitespaces and newlines
        cleaned_string = ''.join(sql.split())

        # Apply regex search for words surrounded by backticks
        pattern = r'`([^`]+)`'
        matches = re.findall(pattern, cleaned_string)

        # Return the second match if it exists, otherwise return None
        if len(matches) >= 2:
            return matches[0], matches[1]

        raise ValueError("Could not extract relations from SQL statement")

    def __post_init__(self):
        self.db_name, self.table_name = self._get_relations_from_sql(self.raw_sql_statement)


def load_create_table_statement(project_root: str, model_paths: list[str], model_name: str) -> str:
    """

    :param project_root:
    :param model_paths:
    :param model_name:
    :return:
    """
    for mp in model_paths:
        _fp = os.path.join(project_root, mp, PRE_CREATE_MODEL_DIR, f"{PRE_CREATE_TEMPLATE_PREFIX}{model_name}")
        if os.path.exists(_fp):
            with open(_fp, "r") as f_in:
                return f_in.read()
    raise dbt.exceptions.DbtRuntimeError(
        f"Could not find table pre-creation SQL code for the following configuration: "
        f"project_root=[{project_root}], model_paths=[{model_paths}], model_name=[{model_name}]"
    )


def is_pre_creatable(sql: str) -> bool:
    """

    :param sql:
    :return:
    """
    # Remove newlines and normalize whitespace
    sql_clean = sql.strip().replace('\n', '')
    sql_clean = re.sub(r'\s+', ' ', sql_clean).strip().lower()

    # TODO : Docs
    suitable_etl_patterns = [
        r'^create\s+table.*select',
    ]

    return any(re.search(pattern, sql_clean) for pattern in suitable_etl_patterns)


def create_pre_create_handler(
        sql: str,
        project_root: str,
        model_paths: list[str],
        models: dict,
) -> PreCreateSQLHandler:
    """

    :param sql:
    :return:
    """
    handler = PreCreateSQLHandler(raw_sql_statement=sql)

    # Prepare the SQL queries
    _create_statement = load_create_table_statement(
        project_root=project_root,
        model_paths=model_paths,
        model_name=f"{handler.model_name}.sql"
    ).format(
        relation_name=f"`{handler.db_name}`.`{handler.table_name}`"
    )
    _relation = f"`{handler.db_name}`.`{handler.table_name}`"
    _cleaned_sql = handler.raw_sql_statement.replace("\n", "")
    _split = _cleaned_sql.split("as select")

    # Extract column names from the config
    insert_column_names = models.get(handler.model_name, {}).get(PRE_CREATE_CONFIG_TAG, {}).get(PRE_CREATE_INSERT_COLUMNS_TAG, [])

    # Set the object values
    handler.config_statement = _split[0].split(f"{_relation}")[1]
    handler.create_statement = _create_statement + handler.config_statement
    handler.insert_statement = f"insert into {_relation} ({','.join(insert_column_names)}) select {_split[1]}"

    return handler