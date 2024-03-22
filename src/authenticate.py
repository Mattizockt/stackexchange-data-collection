import os   
from dotenv import load_dotenv
import requests
import logging

class Authenticate:
    def __init__(self):
        self._access_token = None

        load_dotenv()
        self.authorize()
        self.request_access_token()

    @property
    def access_token(self):
        return self._access_token

    @access_token.setter
    def access_token(self, value):
        self._access_token = value
        
    def authorize(self):
        url = f"https://stackoverflow.com/oauth?client_id={os.environ.get('CLIENT_ID')}&scope=private_info&redirect_uri={os.environ.get('DOMAIN')}"
        print(f"Please go to {url} and copy the code from the search bar\n")

    def request_access_token(self):
        url = "https://stackoverflow.com/oauth/access_token/json"
        code = input("Please input the code: ")
        data = {
            "client_id" : os.environ.get('CLIENT_ID'),
            "client_secret" : os.environ.get('CLIENT_SECRET'),
            "code" : code,
            "redirect_uri" : os.environ.get('DOMAIN')
        }

        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            response_json = response.json()
            self.access_token = response_json["access_token"]
            logging.info(f"Access token successfully requested.")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error when requesting an access token: {e}.")

        except KeyError:
            logging.error(f"Access/Request token is not valid.")


