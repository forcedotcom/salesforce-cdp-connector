# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "pyjwt",
#     "python-dateutil",
#     "python-dotenv",
#     "requests",
#     "urllib3",
# ]
# ///

from os import getenv
from salesforcecdpconnector.connection import SalesforceCDPConnection
from dotenv import load_dotenv

missing = []

def get_required_env(key: str) -> str | None:
    key = key.upper()
    val = getenv(key)
    if not val:
        missing.append(key)
    return val

load_dotenv()

keys = ["login_url", "username", "password", "client_id", "client_secret"]
args = {key: get_required_env(key) for key in keys}
query = get_required_env("QUERY")

if len(missing) > 0:
    raise KeyError(f"Missing required environment variable(s), check your .env file: {missing!r}")

conn = SalesforceCDPConnection(**args)
cursor = conn.cursor()

print(f"executiong query: {query}")
cursor.execute(query)

print("fetchall results:")
print(cursor.fetchall())

print("pandas dataframe:")
print(conn.get_pandas_dataframe(query))