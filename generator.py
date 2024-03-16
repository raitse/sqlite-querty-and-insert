import os
import random
import libsql_experimental as libsql
from dotenv import load_dotenv
from datetime import datetime


def main():
    counter = 0
    print(f"connecting to {os.getenv('LIBSQL_URL')}")
    conn = libsql.connect(
        database=os.getenv("LIBSQL_URL"), auth_token=os.getenv("LIBSQL_AUTH_TOKEN")
    )
    conn.execute("BEGIN TRANSACTION")
    rows = conn.execute("SELECT * FROM users;").fetchall()
    for row in rows:
        user_id = row[0]
        counter += 1
        if counter <= 1:
            user_insert_count = range(3000)
        elif counter == 2:
            user_insert_count = range(1000)
        else:
            random_insert_count = random.randint(100, 200)
            user_insert_count = range(random_insert_count)
        timestamp = datetime.now()
        message = "test_message"
        for _ in user_insert_count:
            conn.execute(
                f"INSERT INTO message (created_at, target_id, message) VALUES ('{timestamp}', '{user_id}', '{message}');"
            )
            conn.execute("END TRANSACTION")
    conn.commit()


load_dotenv()
main()
