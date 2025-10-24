
import os
import sys
import argparse
from dotenv import load_dotenv
load_dotenv()
server_uri = os.getenv("MYSQL_SERVER_URI")
print(f"MYSQL URI: {server_uri}")
if not server_uri:
    print("ERROR: Provide --uri or set MYSQL_SERVER_URI in environment.")
    sys.exit(1)

def show_databases(server_uri: str, check_name=None) -> int:
    """Connect to a MySQL server URI and print databases from SHOW DATABASES.

    If check_name is provided, also report whether it is present in the list
    (case-insensitive), and return non-zero if not present.
    """
    try:
        import sqlalchemy as sa
    except Exception as exc:
        print(f"ERROR: SQLAlchemy not installed: {exc}")
        return 2

    try:
        engine = sa.create_engine(server_uri, pool_pre_ping=True)
        with engine.connect() as conn:
            rows = conn.execute(sa.text("SHOW DATABASES")).fetchall()
            dbs = [r[0] for r in rows]
    except Exception as exc:
        print(f"ERROR: Failed to connect or run SHOW DATABASES: {exc}")
        return 1

    # List all databases
    for name in dbs:
        print(f"- {name}")

    # Optional membership check
    if check_name:
        def _norm(x):
            try:
                if isinstance(x, bytes):
                    x = x.decode('utf-8', errors='ignore')
                return str(x).strip().strip('`"\'').lower()
            except Exception:
                return ''

        target = _norm(check_name)
        present = target in [_norm(d) for d in dbs]
        print(f"Present({check_name}): {present}")
        return 0 if present else 1

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Test SHOW DATABASES against a MySQL server URI")
    parser.add_argument("--uri", dest="uri", help="Server-level MySQL URI (e.g. mysql+pymysql://user:pass@host:3306/)")
    args = parser.parse_args()

    uri = args.uri or os.getenv("MYSQL_SERVER_URI")
    
    if not uri:
        print("ERROR: Provide --uri or set MYSQL_SERVER_URI in environment.")
        return 2

    try:
        name = input("Enter database name to check (press Enter to skip): ").strip()
    except Exception:
        name = ""

    return show_databases(uri, name or None)


if __name__ == "__main__":
    sys.exit(main())


