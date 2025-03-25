import os
import time
import pymysql

# Configure connection
conn = pymysql.connect(
    host=os.environ['LOCUST_MYSQL_HOST'],
    port=int(os.environ['LOCUST_MYSQL_PORT']),
    user=os.environ['LOCUST_MYSQL_USER'],
    password=os.environ['LOCUST_MYSQL_PASSWORD'],
    database=os.environ['LOCUST_MYSQL_DB'],
)


def execute_sequential_tasks():
    with conn.cursor() as cursor:
        for part in range(0, 121):  # 0-120 inclusive
            # Generate SQL with current part number
            # Get task ID (implementation may vary)
            task_name = f"filter_part_{part}"  # Adjust based on your naming

            sql = f"""
            SUBMIT /*+set_var(query_timeout=100000)*/ TASK {task_name} AS
            INSERT INTO kf_consequences_filter_part_10 
            SELECT
                {part} AS part,
                c.*
            FROM
                kf_consequences_filter_10 c 
            LEFT SEMI JOIN kf_occurrences_10 o 
                ON c.locus_id = o.locus_id 
                AND part = {part};
            """

            # Execute task submission
            cursor.execute(sql)
            conn.commit()

            # Monitor task completion
            while True:
                cursor.execute(f"""
                    SELECT state 
                    FROM information_schema.task_runs 
                    WHERE task_name = '{task_name}'
                    ORDER BY CREATE_TIME DESC 
                    LIMIT 1
                """)
                result = cursor.fetchone()

                if result and result[0] == 'SUCCESS':
                    print(f"Part {part} completed successfully")
                    break
                elif result and result[0] == 'FAILED':
                    raise Exception(f"Task failed for part {part}")

                print(f"Waiting for part {part}...")
                time.sleep(5)  # Check every 5 seconds


if __name__ == "__main__":
    execute_sequential_tasks()
    conn.close()