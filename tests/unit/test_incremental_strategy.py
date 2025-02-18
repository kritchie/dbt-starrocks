from dbt.context.providers import generate_runtime_model_context
from dbt.tests.util import get_manifest, run_dbt
import pytest


@pytest.fixture(scope="class")
def models():
    return {"my_model.sql": "select 1 as fun"}


@pytest.mark.parametrize("incremental_strategy, target_macro_name", [
    ("default", "dbt_macro__get_incremental_default_sql"),
    ("insert_overwrite", "dbt_macro__get_incremental_insert_overwrite_sql"),
    ("dynamic_overwrite", "dbt_macro__get_incremental_dynamic_overwrite_sql"),
])
def test__incremental_strategy_targets_the_right_macro(project, incremental_strategy, target_macro_name):
    run_dbt(["run"])  # Necessary to generate the manifest
    manifest = get_manifest(project.project_root)
    model = manifest.nodes["model.test.my_model"]

    context = generate_runtime_model_context(
        model,
        project.adapter.config,
        manifest,
    )

    macro_func = project.adapter.get_incremental_strategy_macro(context, incremental_strategy)
    assert macro_func.get_macro().name == target_macro_name
