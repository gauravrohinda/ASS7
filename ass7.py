import psycopg2

try:
    # 1. Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="testdb",
        user="postgres",
        password="Test@123",
        host="localhost",
        port="6021"
    )
    print("Database connected successfully!")

    cur = conn.cursor()

    # 2. Create Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            age INT
        );
    """)
    conn.commit()
    print("Table created successfully!")

    # 3. Insert Records
    cur.execute("INSERT INTO students (name, age) VALUES (%s, %s)", ('John', 22))
    cur.execute("INSERT INTO students (name, age) VALUES (%s, %s)", ('Alice', 20))
    cur.execute("INSERT INTO students (name, age) VALUES (%s, %s)", ('Bob', 25))
    conn.commit()
    print("Records inserted successfully!")

    # 4. Read Records
    print("\nFetching all records:")
    cur.execute("SELECT * FROM students;")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    # 5. Update Record
    cur.execute("UPDATE students SET age = %s WHERE name = %s", (23, 'John'))
    conn.commit()
    print("\nRecord updated successfully!")

    # Verify update
    cur.execute("SELECT * FROM students WHERE name = %s;", ('John',))
    print("Updated Record:", cur.fetchone())

    # 6. Delete Record
    cur.execute("DELETE FROM students WHERE name = %s", ('Alice',))
    conn.commit()
    print("\nRecord deleted successfully!")

    # Verify deletion
    cur.execute("SELECT * FROM students;")
    print("Remaining Records after deletion:")
    for row in cur.fetchall():
        print(row)

    # 7. Exception Handling & Rollback
    try:
        cur.execute("SELECT * FROM non_existing_table;")  # This will throw an error
    except Exception as e:
        print("\nError handled gracefully:", e)
        conn.rollback()

    # Close connection
    cur.close()
    conn.close()
    print("\nConnection closed.")

except Exception as e:
    print("Error:", e)
