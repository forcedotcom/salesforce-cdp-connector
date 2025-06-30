import salesforcecdpconnector as sfcdp
from loguru import logger
import sys

# Configure logging (optional, but recommended)
logger.remove()
logger.add(sys.stderr, level="DEBUG") # Set desired level (DEBUG, INFO, etc.)

# Connection details (replace with your actual credentials)
# CLIENT_ID = "3MVG9.NSlnJa6holhEzHEIXqEfJTLmpixLBhGQ7eFNtHPXSCTo3WDRyAq7navppCqlOiRM_aX8NfxXP7OqFnA",
# CLIENT_SECRET = "9068265227C98A7A9608ABFE73EAF4DBFF62449C762A2DCA663662EA431E9F9F",
# USERNAME = "dna_forge@orge360.com.e360dev",
# PASSWORD = "aO~t}0iK063vUs0XMPAd",
# LOGIN_URL = "https://orge360--e360dev.sandbox.my.salesforce.com/"
#
# CLIENT_ID = "3MVG9l3R9F9mHOGYEw_nsLPfYdOhUieRtFjW2m.mAiYGC4yI.Ayrvzw5M55R_beLKvnmQKBe6o1ZgEyWJayOT"
# CLIENT_SECRET = "704728509EE9DC0416CFE0A9053A083D97276B6D84AE0FFAEFF874DCB513AFC3"
# USERNAME = "epic.639ef2250c93@orgfarm.out"
# PASSWORD = "orgfarm1234"
# LOGIN_URL = "https://orgfarm-655ee816cb.test1.my.pc-rnd.salesforce.com"

LOGIN_URL = "trailsignup-972ab8bf72a9c7.my.salesforce.com"
CLIENT_ID = "3MVG9Rr0EZ2YOVMbMrq7Rcmf5QoZoKZ1MHHR_.HZPBGp0JD3JfGb4okY.QUczDqQrU_9h9HSmW7YdaKOQ3c8C"
CLIENT_SECRET = "81764CA11340110300E34529987B1C701A5D766B0D5E42DDE496A9BD95BC18E3"

try:
    # Connect using the PEP 249 connect function
    conn = sfcdp.connect(
        login_url=LOGIN_URL,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        # username=USERNAME,  # Uncomment if using username/password auth
        # password=PASSWORD   # Uncomment if using username/password auth
    )

    # Use context manager for cursor
    with conn.cursor() as cur:
        # Example query - Use named parameters if paramstyle='named'
        # Adjust SQL and parameters based on your CDP objects/fields
        
        # Complex query that will generate a large dataset
        sql = """
        SELECT 
            a.acct_id__c,
            a.activation_rate__c,
            a.engagement_rate__c,
            a.penetration_rate__c,
            a.utilization_rate__c,
            b.acct_id__c as b_acct_id,
            b.activation_rate__c as b_activation_rate,
            b.engagement_rate__c as b_engagement_rate,
            b.penetration_rate__c as b_penetration_rate,
            b.utilization_rate__c as b_utilization_rate
        FROM product_metrics__dll a
        CROSS JOIN product_metrics__dll b
        WHERE a.acct_id__c IS NOT NULL 
        AND b.acct_id__c IS NOT NULL
        ORDER BY a.acct_id__c, b.acct_id__c
        """
        
        # Alternative: Self-join with different conditions
        # sql = """
        # SELECT 
        #     a.acct_id__c,
        #     a.activation_rate__c,
        #     a.engagement_rate__c,
        #     b.acct_id__c as b_acct_id,
        #     b.activation_rate__c as b_activation_rate,
        #     b.engagement_rate__c as b_engagement_rate
        # FROM product_metrics__dll a
        # INNER JOIN product_metrics__dll b ON a.acct_id__c != b.acct_id__c
        # WHERE a.activation_rate__c > 0 AND b.activation_rate__c > 0
        # ORDER BY a.acct_id__c, b.acct_id__c
        # """
        
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
        for i, row in enumerate(all_rows):
             logger.info(f"Row {i+1}: {row}") # Rows are tuples

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