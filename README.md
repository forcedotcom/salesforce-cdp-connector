# salesforce-cdp-connector

A readonly CDP client for Python. This can be used to execute queries against CDP and load the data into python.

## Usage

### Installation

Install the package from Pypi using the command

```
pip install salesforce-cdp-connector
```

### Quick Start

We have to create an instance of the SalesforceCDPConnection to connect to CDP.

The object can be created as follows:

**Using Username and Password**
```
from salesforcecdpconnector.connection import SalesforceCDPConnection
conn = SalesforceCDPConnection(
        'login_url', 
        'user_name', 
        'password', 
        'client_id', 
        'client_secret'
    )
```

**Using OAuth Tokens**
```
from salesforcecdpconnector.connection import SalesforceCDPConnection
conn = SalesforceCDPConnection(login_url, 
       client_id='<client_id>', 
       client_secret='<client_secret>', 
       core_token='<core token>'
       refresh_token='<refresh_token>'
   )
```

Once the connection object is created the queries can be executed using cursor as follows

```
cur = conn.cursor()
cur.execute('<query>')
results = cur.fetchall()
```

The query results can also be directly extracted as a pandas dataframe

```
dataframe = conn.get_pandas_dataframe('<query>')
```

### Creating a connected App

1. Log in to salesforce as an admin. In the top right corner, click on the gear icon and go to step
2. In the left hand side, under Platform Tools, go to Apps > App Manager
3. Click on New Connected App
4. Fill in the required Basic Information fields.
5. Under API (Enable OAuth Settings)
    1. Click on the checkbox to Enable OAuth Settings.
    2. Provide a callback URL.
    3. In the Selected OAuth Scopes, make sure that refresh_token, api, cdp_query_api, cdp_profile_api is selected.
    4. Click on Save to save the connected app
6. From the page that opens up, click on the Manage Consumer Details to find your client id and client secret

### Fetching Refresh Token

1. From the connected app, note down the below details:
   * Client Id
   * Client Secret
   * Callback URL
2. Obtain the code
   1. From browser, go to the below url.
   ```
   <LOGIN_URL>/services/oauth2/authorize?response_type=code&client_id=<client_id>&redirect_uri=<callback_url>
   ```
   2. This will redirect you to the callback url. The redirected url will be of the form
   ```<callback url>?code=<CODE>```
   3. Extract the CODE from the address bar to be used in next step. Check the network tab of browser if the addressbar doesn't show this.
   
3. Get core and refresh tokens
   1. Make a post call using curl or postman to the below url using the code retrieved in previous step.
   ```
   <LOGIN_URL>/services/oauth2/token?code=<CODE>&grant_type=authorization_code&client_id=<clientId>&client_secret=<clientSecret>&redirect_uri=<callback_uri>
   ```
   2. The response to the above post call will be a json with access_token and refresh_token
