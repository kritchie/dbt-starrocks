import pytest

from dbt.adapters.starrocks.helpers.pre_create import (
    is_pre_creatable,
)


class TestPreCreate:

    @pytest.mark.parametrize("sql", [
        # DBT-like statements
        """
  
    

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

payments as (

    select * from `jaffle_shop`.`stg_payments`

),

customer_orders as (

        select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(amount) as total_amount

    from payments

    left join orders on
         payments.order_id = orders.order_id

    group by orders.customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value

    from customers

    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id

)

select * from final"""
    ])
    def test_is_pre_creatable(self, sql):
        assert is_pre_creatable(sql) == True

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
