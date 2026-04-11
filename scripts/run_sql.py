from pathlib import Path
from sqlalchemy import text
from db import get_engine


def execute_sql_file(file_path: str):
    sql_text = Path(file_path).read_text(encoding="utf-8")
    engine = get_engine()

    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    print(f"[OK] Executed SQL file: {file_path}")


if __name__ == "__main__":
    import sys
    file_path = sys.argv[1]
    execute_sql_file(file_path)