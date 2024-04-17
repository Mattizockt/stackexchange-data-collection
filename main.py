import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="basic.log",
    force=True
)
from src import authenticate, sqlManager, apiCall

obj0 = authenticate.Authenticate()
obj1 = apiCall.APIcall(obj0.access_token)

# obj1 = sqlManager.SQLManager()