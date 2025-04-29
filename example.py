import salesforcecdpconnector as sfcdp
from loguru import logger
import sys

# Configure logging (optional, but recommended)
logger.remove()
logger.add(sys.stderr, level="DEBUG") # Set desired level (DEBUG, INFO, etc.)

# Connection details (replace with your actual credentials)
CLIENT_ID = "3MVG9.NSlnJa6holhEzHEIXqEfJTLmpixLBhGQ7eFNtHPXSCTo3WDRyAq7navppCqlOiRM_aX8NfxXP7OqFnA",
CLIENT_SECRET = "9068265227C98A7A9608ABFE73EAF4DBFF62449C762A2DCA663662EA431E9F9F",
USERNAME = "dna_forge@orge360.com.e360dev",
PASSWORD = "aO~t}0iK063vUs0XMPAd",
LOGIN_URL = "https://orge360--e360dev.sandbox.my.salesforce.com/"

try:
    # Connect using the PEP 249 connect function
    conn = sfcdp.connect(
        login_url=LOGIN_URL,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        username=USERNAME,
        password=PASSWORD
    )
    logger.info("Connection successful!")

    # Use context manager for cursor
    with conn.cursor() as cur:
        logger.info("Executing query...")
        # Example query - Use named parameters if paramstyle='named'
        # Adjust SQL and parameters based on your CDP objects/fields
        sql = "select * from Forge_Snowflake_Account_D84C1BE7__dll LIMIT 10"
        # params = {"last_name": "Smith"}

        # If using paramstyle='qmark':
        # sql = "SELECT FirstName__c, Email__c FROM UnifiedIndividual__dlm WHERE LastName__c = ? LIMIT 10"
        # params = ("Smith",)

        cur.execute(sql)
        logger.info(f"Query submitted. Cursor description: {cur.description}")

        # Fetch results
        # Option 1: Fetch one
        # row = cur.fetchone()
        # logger.info(f"First row: {row}")

        # Option 2: Fetch many
        # rows = cur.fetchmany(5)
        # logger.info(f"Fetched {len(rows)} rows: {rows}")

        # Option 3: Fetch all
        all_rows = cur.fetchall()
        logger.info(f"Fetched all {len(all_rows)} rows.")
        for row in all_rows:
             logger.info(f" Row: {row}") # Rows are tuples

        logger.info(f"Final rowcount: {cur.rowcount}") # Should be updated after fetchall

        # Example of trying an unsupported operation
        try:
            conn.rollback()
        except sfcdp.NotSupportedError as e:
            logger.warning(f"Caught expected error: {e}")

except sfcdp.AuthenticationError as e:
    logger.error(f"Authentication failed: {e}")
except sfcdp.QueryError as e:
    logger.error(f"Query execution failed: {e}")
except sfcdp.DatabaseError as e:
    logger.error(f"A database error occurred: {e}")
except Exception as e:
    logger.exception("An unexpected error occurred.")
finally:
    # Connection is closed automatically if using 'with conn:'
    # otherwise, ensure conn.close() is called
    if 'conn' in locals() and conn and not getattr(conn, '_is_closed', True):
        conn.close()
        logger.info("Connection closed manually.")

# from salesforcecdpconnector.dbapi import SalesforceCDPConnection

# connection = SalesforceCDPConnection(
#     client_id = "3MVG9.NSlnJa6holhEzHEIXqEfJTLmpixLBhGQ7eFNtHPXSCTo3WDRyAq7navppCqlOiRM_aX8NfxXP7OqFnA",
#     client_secret = "9068265227C98A7A9608ABFE73EAF4DBFF62449C762A2DCA663662EA431E9F9F",
#     username = "dna_forge@orge360.com.e360dev",
#     password = "aO~t}0iK063vUs0XMPAd",
#     login_url = "https://orge360--e360dev.sandbox.my.salesforce.com/"
# )

# with connection.cursor() as cursor:
#     cursor.execute("select * from Forge_Snowflake_Account_D84C1BE7__dlls LIMIT 10")
#     print(cursor.fetchmany())

#     # for row in cursor:
#     #     print(row)

