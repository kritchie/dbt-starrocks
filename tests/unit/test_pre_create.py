import pytest

from dbt.adapters.starrocks.helpers.pre_create import (
    is_pre_creatable, split_config_select,
)


class TestPreCreate:
    # DBT-like statements
    complex_dbt_str = """



  create table `jaffle_shop`.`customers__dbt_tmp`
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

with customers as (

    select * from `jaffle_shop`.`stg_customers`

),

orders as (

    select * from `jaffle_shop`.`stg_orders`

),

final as (

    select
        customers.customer_id,
        customers.first_name,

    from customers

    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

)

select * from final"""

    @pytest.mark.parametrize("sql", [complex_dbt_str])
    def test_is_pre_creatable(self, sql):
        assert is_pre_creatable(sql) == True

    @pytest.mark.parametrize("sql", [complex_dbt_str])
    def test_split_ok(self, sql):
        config_split, select_split = split_config_select(sql)
        assert config_split == '  create table `jaffle_shop`.`customers__dbt_tmp`    PROPERTIES (      "replication_num" = "1"    )  '
        assert select_split == (
            'as '
            'with  customers as ('
            '    select * from `jaffle_shop`.`stg_customers`'
            '),'
            'orders as ('
            '    select * from `jaffle_shop`.`stg_orders`'
            '),'
            'final as ('
            '    select'
            '        customers.customer_id,'
            '        customers.first_name,'
            '    from customers'
            '    left join customer_orders'
            '        on customers.customer_id = customer_orders.customer_id'
            ')'
            'select * from final'
        )
    @pytest.mark.parametrize("sql", [
        # DBT-like statements
        """select
          null as "database",
          tbl.table_name as name,
          tbl.table_schema as "schema",
          case when tbl.table_type = 'BASE TABLE' then 'table'
               when tbl.table_type = 'VIEW' and mv.table_name is null then 'view'
               when tbl.table_type = 'VIEW' and mv.table_name is not null then 'materialized_view'
               when tbl.table_type = 'SYSTEM VIEW' then 'system_view'
               else 'unknown' end as table_type
        from information_schema.tables tbl
        left join information_schema.materialized_views mv
        on tbl.TABLE_SCHEMA = mv.TABLE_SCHEMA
        and tbl.TABLE_NAME = mv.TABLE_NAME
        where tbl.table_schema = 'jaffle_shop'
        """,
        """create view `jaffle_shop`.`stg_customers__dbt_tmp` as 

            with source as (

                select * from `jaffle_shop`.`raw_customers`

            ),

            renamed as (

                select
                    id as customer_id,
                    first_name,
                    last_name

                from source

            )

            select * from renamed;
        """,
        """select VIEW_DEFINITION as sql from information_schema.views where TABLE_SCHEMA='jaffle_shop' and TABLE_NAME='stg_customers'""",
        """drop view if exists `jaffle_shop`.`stg_customers`""",
        """


      create view `jaffle_shop`.`stg_customers__dbt_backup` as WITH `source` (`id`, `first_name`, `last_name`) AS (SELECT `jaffle_shop`.`raw_customers`.`id`, `jaffle_shop`.`raw_customers`.`first_name`, `jaffle_shop`.`raw_customers`.`last_name`
FROM `jaffle_shop`.`raw_customers`) , `renamed` (`customer_id`, `first_name`, `last_name`) AS (SELECT `source`.`id` AS `customer_id`, `source`.`first_name`, `source`.`last_name`
FROM `source`) SELECT `renamed`.`customer_id`, `renamed`.`first_name`, `renamed`.`last_name`
FROM `renamed`
        """,
        """       alter table `jaffle_shop`.`customers` rename customers__dbt_backup
        """,
        """create table `jaffle_shop`.`raw_payments` (
        `id` integer,
        `order_id` integer,
        `payment_method` string,
        `amount` integer
    ) ENGINE = OLAP 
    PROPERTIES (
      "replication_num" = "1"
    )""",
    ])
    def test_is_not_pre_creatable(self, sql):
        assert is_pre_creatable(sql) == False
