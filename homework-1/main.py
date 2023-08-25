"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


if __name__ == '__main__':

    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='qwerty')

    try:
        with conn:
            with conn.cursor() as cur:
                with open('/home/elena/py_project/postgres-homeworks/homework-1/north_data/employees_data.csv', newline='') as csvfile:
                    reader = csv.DictReader(csvfile)

                    for row in reader:
                        cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (
                            row['employee_id'], row['first_name'], row['last_name'], row['title'], row['birth_date'],
                            row['notes'] ))

                with open('/home/elena/py_project/postgres-homeworks/homework-1/north_data/customers_data.csv',
                                  newline='') as csvfile:
                    reader = csv.DictReader(csvfile)

                    for row in reader:
                        cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (
                            row['customer_id'], row['company_name'], row['contact_name']))

                with open('/home/elena/py_project/postgres-homeworks/homework-1/north_data/orders_data.csv',
                                  newline='') as csvfile:
                    reader = csv.DictReader(csvfile)

                    for row in reader:
                        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", (
                            row['order_id'], row['customer_id'], row['employee_id'], row['order_date'], row['ship_city']))

    finally:
        conn.close()
