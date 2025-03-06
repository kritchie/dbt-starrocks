import dataclasses
import pathlib
import re
from typing import Optional

from dbt.exceptions import DbtRuntimeError


PRE_CREATE_CONFIG_TAG = "+pre_create"
PRE_CREATE_INSERT_COLUMNS_TAG = "insert_columns"
PRE_CREATE_MODEL_DIR = "pre_create"
PRE_CREATE_TEMPLATE_PREFIX = "template_"


@dataclasses.dataclass
class PreCreateSQLAdapter:
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
        Extracts relation names from a SQL statement.

        Assumes standard relations using backtick symbol "`"

        :param sql: The SQL statement to process.
        :return: A tuple of relation objects (usually db name, table name)
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


def load_create_table_statement(project_root: str, model_paths: list[str], model_file: str, relation_name: str) -> str:
    """
    Loads the `CREATE TABLE` SQL statement from a predefined file.

    This function searches for a `.sql` file containing only the table creation statement
    (without table properties) for a specific dbt model. The file must follow a specific
    naming convention and reside within one of the configured model paths.

    Example of expected SQL content in the file:

        CREATE TABLE my_table (
            col1 VARCHAR(24),
            col2 INT,
            col3 INT AUTO_INCREMENT
        )

    **Note:** Only include the table creation statement without any additional properties
    like table options.

    :param project_root: The root directory of the dbt project.
    :param model_paths: A list of paths (relative to the project root) where dbt models are located.
    :param model_file: The filename of the dbt model for which to load the pre-create SQL statement.
    :param relation_name: The relation name (db + table) to inject inside the SQL statement.
    :return: The SQL query loaded from the file.
    :raises dbt.exceptions.DbtRuntimeError: If no matching SQL file is found.
    """
    for mp in model_paths:
        _fp = pathlib.Path(project_root) / mp / PRE_CREATE_MODEL_DIR / f"{PRE_CREATE_TEMPLATE_PREFIX}{model_file}"
        if _fp.exists():
            _statement = _fp.read_text()
            if "{relation_name}" not in _statement:
                raise ValueError(
                    f"You Pre-Create SQL statement must use the '{{relation_name}}' placeholder as the table relation."
                )
            return _statement.format(relation_name=relation_name)
    raise DbtRuntimeError(
        "Could not find table pre-creation SQL code for the following configuration: "
        f"project_root=[{project_root}], model_paths=[{model_paths}], model_file=[{model_file}]"
    )


def is_pre_creatable(sql: str) -> bool:
    """
    Evaluates if the SQL string is suitable for pre-creation.

    :param sql: The SQL statement to process.
    :return: True if the SQL contains a pre-creatable statement.
    """
    # Remove newlines and normalize whitespace
    sql_clean = sql.strip().replace('\n', '')
    sql_clean = re.sub(r'\s+', ' ', sql_clean).strip().lower()

    # Note: # Pre-Creatable statements are only CREATE TABLE ... AS SELECT
    # https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT/
    #
    # The goal here is to soft-match the pattern for dbt-generated sql queries.
    # It is not intended to be exhaustive nor to validate SQL statement, this will be left to the engine.
    pre_create_pattern = r'^create\s+table.*select'
    return bool(re.search(pre_create_pattern, sql_clean))


def create_adapter(
    sql: str,
    project_root: str,
    model_paths: list[str],
    models: dict,
) -> Optional[PreCreateSQLAdapter]:
    """
    Creates a SQL adapter for pre-create operations.

    :param sql: The raw SQL statement to process.
    :param project_root: The root directory of the dbt project.
    :param model_paths: A list of paths (relative to the project root) where dbt models are located.
    :param models: The configuration object of the dbt models.
    :return: Configured PreCreateSQLAdapter instance.
    """
    if not is_pre_creatable(sql=sql):
        # We don't need to pre-create, it's not a suitable SQL statement.
        return None

    # Parse the SQL
    handler = PreCreateSQLAdapter(raw_sql_statement=sql)
    _relation = f"`{handler.db_name}`.`{handler.table_name}`"
    _clean_split = handler.raw_sql_statement.replace("\n", "").split("as select")
    if len(_clean_split) != 2:
        raise ValueError("Invalid SQL structure - missing `as select` clause")

    if not models.get(handler.model_name, {}).get(PRE_CREATE_CONFIG_TAG):
        # We don't need to pre-create, the `pre_create` setting was not set.
        return None

    # Prepare the SQL queries
    _create_statement = load_create_table_statement(
        project_root=project_root,
        model_paths=model_paths,
        model_file=f"{handler.model_name}.sql",
        relation_name=_relation
    )

    # Extract column names from the config
    insert_column_names = models.get(handler.model_name, {}).get(PRE_CREATE_CONFIG_TAG, {}).get(PRE_CREATE_INSERT_COLUMNS_TAG, [])

    # Set the object values
    handler.config_statement = _clean_split[0].split(f"{_relation}")[1]
    handler.create_statement = _create_statement + handler.config_statement
    handler.insert_statement = f"insert into {_relation} ({','.join(insert_column_names)}) select {_clean_split[1]}"

    return handler