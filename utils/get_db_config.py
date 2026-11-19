"""Print one database connection from config/db.ini, space separated:

    ip port username encoded_password database

Usage: python3 utils/get_db_config.py dwh_prod
"""

import sys
from configparser import ConfigParser
from pathlib import Path

CONFIG_FILE = Path(__file__).resolve().parents[1] / "config" / "db.ini"

FIELDS = ["ip", "port", "username", "encoded_password", "database"]


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: get_db_config.py <section>")

    config = ConfigParser()
    if not config.read(CONFIG_FILE):
        sys.exit(f"missing {CONFIG_FILE} (copy config/db.ini.example)")

    section = config[sys.argv[1]]
    print(" ".join(section[field] for field in FIELDS))


if __name__ == "__main__":
    main()
