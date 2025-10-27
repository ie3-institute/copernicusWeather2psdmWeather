"""
Database migration utilities for converting time column format.
"""

from sqlalchemy import text

from weather.database import engine

from .timer import timer


def migrate_time_column():
    """
    Migrate weathervalue table to convert the time column from string to timestamptz.

    This migration is necessary since we expect the time column in the database to be in format timestamp with time zone `timestamptz` and to be used as primary key.
    To achieve this, the time string from input data is used and converted by the following steps:
    1. Create a temporary table with the same structure
    2. Alter the temporary table to use timestamptz for the time column
    3. Copy data with conversion from string to timestamptz
    4. Add primary key constraint
    5. Rename tables (atomic operation)
    6. Drop the old table
    """
    with timer("Database migration"):
        try:
            print("Starting database migration to convert time column")

            # SQL statements to execute
            migration_steps = [
                # 1. Create new temporary table
                """
                CREATE TABLE weathervalue_temp AS
                SELECT * FROM weathervalue WHERE 1=0
                """,
                # 2. Alter the temporary table to have the correct column type
                """
                ALTER TABLE weathervalue_temp
                ALTER COLUMN time TYPE timestamptz
                USING (REPLACE(time::text, 'Z', '+00')::timestamptz)
                """,
                # 3. Copy the data with the conversion
                """
                INSERT INTO weathervalue_temp
                SELECT
                  (REPLACE(time::text, 'Z', '+00')::timestamptz) as time,
                  coordinate_id, aswdifd_s, aswdir_s, t2m, u131m, v131m
                FROM weathervalue
                """,
                # 4. Add primary key constraint to the temporary table
                """
                ALTER TABLE weathervalue_temp
                ADD PRIMARY KEY (time, coordinate_id)
                """,
                # 5. Rename the tables (atomic operation in PostgreSQL)
                """
                ALTER TABLE weathervalue RENAME TO weathervalue_old
                """,
                """
                ALTER TABLE weathervalue_temp RENAME TO weathervalue
                """,
                # 6. Drop the old table
                """
                DROP TABLE weathervalue_old
                """,
            ]

            # Execute each SQL statement
            with engine.begin() as conn:
                for i, sql in enumerate(migration_steps):
                    print(f"Executing migration step {i + 1}/{len(migration_steps)}")
                    conn.execute(text(sql))

            print("Database migration completed successfully")

        except Exception as e:
            print(f"Migration failed: {e}")
            raise
