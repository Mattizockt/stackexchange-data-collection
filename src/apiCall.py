import logging
from dotenv import load_dotenv
import os   
import requests
from stackapi import StackAPI, StackAPIError

class APIcall:
    def __init__(self, access_token=None):
        self._access_token = access_token
        load_dotenv()

        try:
            self.SITE = StackAPI('stackoverflow', key=os.environ.get('KEY'))
            self.SITE.max_pages = 1
            self.SITE.page_size = 50
            # self.SITE._api_key = os.environ.get('KEY')
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")   
            print(f"Error {e.error} when fetching from API: {e.message}")

        # TODO remove later
        for i in range(2):
            self.get_users(i)

    # TODO fetches the most popular users only yet. later, add option to fetch by username
    def get_users(self, page_num):
        ft = "!BTeL)VWxnY-BAFRYwkG.fXkZFs.WRj"
        users = None

        try:
            users = self.SITE.fetch("users", sort="reputation", order="desc", filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")

        return users

    def get_question_object(self, question_id):
        # TODO 
        NotImplementedError()

    def get_answer_object(self, answer_id):
        # TODO 
        NotImplementedError()

    # TODO make more sophisticated later
    def _make_api_call(self, url, params):
        req_json = None
        try:
            request = requests.post(url, params=params)

            if request.status_code == 429:
                logging.error(f"Rate limit exceeded.")
            elif request.status_code == 404:
                logging.error(f"Server returned 404 with url {url} with params {params}.")
            else:
                logging.info(f"Api call with url {url} with params {params} successfull")
                req_json = request.json()

                # TODO in an unsucessfull request, json fie with error_message, error_id is returned    

        except requests.RequestException as e:
            logging.error(f"Error during request: {e}")

        return req_json

obj1 = APIcall()