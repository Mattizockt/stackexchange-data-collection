import logging
from dotenv import load_dotenv
import os   
import requests
from stackapi import StackAPI, StackAPIError

from .sqlManager import SQLManager

# start date: 1625097600
# end date: 1688169600
class APIcall:
    def __init__(self, access_token=None):
        self._access_token = access_token
        self.START_DATE = 1625097600
        self.END_DATE = 1688169600
        load_dotenv()

        try:
            self.SITE = StackAPI('stackoverflow', key=os.environ.get('KEY'), access_token=self._access_token)
            self.SITE.max_pages = 1
            self.SITE.page_size = 50
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")   
            print(f"Error {e.error} when fetching from API: {e.message}")

        x = SQLManager("localhost", "root" ,"password", "stackexchange")

        for u in x.get_inserted_users():
            questions = self.get_answers(u)
            for question in questions:
                x.insert_into_answers_table(question)

    # TODO fetches the most popular users only yet. later, add option to fetch by username
    # parameter is the number of pages (50 users each) to fetch
    def get_users(self, page_num):
        ft = "!BTeL)VWxnY-BAFRYwkG.fXkZFs.WRj"
        users = None

        try:
            users = self.SITE.fetch("users", sort="reputation", order="desc", filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")

        return users

    # from 2021.7 to 2023.7
    def get_questions(self, user_id):
        ft = "!*IU7fu9q*(HUyC)4G0n-hheLcFNuneWVNABPhspqDFV9K9h-FeiU5ePAWuclOc"
        questions = None

        try:
            questions = self.SITE.fetch(f"users/{user_id}/questions", fromdate=self.START_DATE, todate=self.END_DATE, sort="votes", order="desc", filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")
        
        return questions["items"]
        
    # gets 50 answers per users between 2021.7 to 2023.7
    def get_answers(self, user_id):
        ft = "!D4oe0pk494TPqmPIxMhLQQhXC8w3TLywsgaR33Cqg58tJt6_I(i"
        answers = None

        try:
            answers = self.SITE.fetch(f"users/{user_id}/answers", sort="votes", order="desc", fromdate=self.START_DATE, todate=self.END_DATE, filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")

        return answers["items"]
