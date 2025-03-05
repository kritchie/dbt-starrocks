import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, check_relations_equal, relation_from_name

from dbt.adapters.starrocks.helpers.pre_create import create_handler

seed_a_csv = """
id,value
1,a
2,b
3,c
4,d
5,e
""".lstrip()

groundtruth_csv = """
id,value,uid
1,a,1
2,b,2
3,c,3
4,d,4
5,e,5
""".lstrip()

model_a_csv = """
{{ config(materialized='table') }}

select * from {{ ref('seed_a') }}
""".lstrip()

model_a_pre_create_csv = """
create table if not exists {relation_name} (
    id BIGINT,
    value VARCHAR(5),
    uid BIGINT AUTO_INCREMENT
)
"""

class TestSubmitTaskModel:

    @staticmethod
    def _seeds():
        return {
            "seed_a.csv": seed_a_csv,
            "groundtruth.csv": groundtruth_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_submit_task",
            "models": {
                "model_a": {
                    "+pre_create": {
                        "insert_columns": ["id", "value"]
                    }
                }
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return self._seeds()

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model_a_csv,
            "pre_create": {
                "template_model_a.sql": model_a_pre_create_csv
            }
        }

    def _seed_assertions(self, project):
        _seeds_row_counts = [
            ("seed_a", 5),
            ("groundtruth", 5),
        ]

        # seed command
        results = run_dbt(["seed"])
        assert len(results) == len(_seeds_row_counts)

        # Make sure seeds are properly setup
        for pname, pcount in _seeds_row_counts:
            relation = relation_from_name(project.adapter, f"{pname}")
            result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
            assert result[0] == pcount


    def _doc_tests(self):
        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == len(self._seeds()) + 1


    def test_create_handler(self, project):
        def clean(s: str) -> str:
            return s.replace("\n", "").replace(" ", "")

        handler = create_handler(
            sql="""
            create table if not exists `doesntmatter`.`model_a__dbt_tmp`
            DISTRIBUTED BY (`key`)
            BUCKETS 5
            PROPERTIES ("foo" = "bar", "alice": "bob")   
            as select  * from `doesntmatter`.`seed_a`"
            """,
            project_root=project.adapter.config.project_root,
            model_paths=project.adapter.config.model_paths,
            models=project.adapter.config.models,
        )
        assert handler.model_name == "model_a"
        assert handler.table_name == "model_a__dbt_tmp"
        assert clean(handler.config_statement) == clean(
            'DISTRIBUTED BY (`key`) BUCKETS 5 PROPERTIES ("foo" = "bar", "alice": "bob")'
        )
        assert clean(handler.create_statement) == clean("""
            create table if not exists `doesntmatter`.`model_a__dbt_tmp` (
                id BIGINT,
                value VARCHAR(5),
                uid BIGINT AUTO_INCREMENT
            )
            DISTRIBUTED BY (`key`)            
            BUCKETS 5            
            PROPERTIES ("foo" = "bar", "alice": "bob")
            """)
        assert clean(handler.insert_statement) == clean(
            'insert into `doesntmatter`.`model_a__dbt_tmp` (id,value) select * from `doesntmatter`.`seed_a`"'
        )

    def test_pre_create_dbt_run(self, project):
        self._seed_assertions(project=project)

        results, stdout = run_dbt_and_capture(["run", "--select", "model_a"])
        assert len(results) == 1
        check_relations_equal(project.adapter, ["groundtruth", "model_a"])

        self._doc_tests()
