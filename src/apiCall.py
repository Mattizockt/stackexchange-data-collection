import logging
from dotenv import load_dotenv
import os   
from stackapi import StackAPI, StackAPIError

from .sqlManager import SQLManager

class APIcall:
    def __init__(self, access_token=None):

        # from start to release: 01/11/2021 - 29/11/2022
        # 
        # from 30.11.2022 - 31.12.2023

        self._access_token = access_token
        self.START_DATE_TREATMENT = 1635696000
        self.END_DATE_TREATMENT = 1669737599
        self.START_DATE_CONTROL = 1669737600
        self.END_DATE_CONTROL = 1704038399
        load_dotenv()

        try:
            self.SITE = StackAPI('stackoverflow', key=os.environ.get('KEY'), access_token=self._access_token)
            self.SITE.max_pages = 1
            self.SITE.page_size = 100
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")   
            print(f"Error {e.error} when fetching from API: {e.message}")

        x = SQLManager("localhost", "root" ,"password", "stackexchange")

        for i in range(1, 1000):
            for user in self.get_users(i):
                x.insert_into_users_table(user)

        # for u in x.get_inserted_users():    # gets 50 answers per users between 2021.7 to 2023.7
        #     questions = self.get_questions(u)
        #     for question in questions:
        #         x.insert_into_questions_table(question)

        #     answers = self.get_answers(u)
        #     for answer in answers:
        #         x.insert_into_answers_table(answer)

    # parameter is the number of pages (50 users each) to fetch
    def get_users(self, page_num):
        ft = "!BTeL)VWxnY-BAFRYwkG.fXkZFs.WRj"
        users = None

        try:
            users = self.SITE.fetch("users", sort="reputation", order="desc", page=page_num, pagesize=100, filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")

        return users["items"]

    # TODO change date
    # from 01/11/2021 to 31.12/2023 
    # release date: 30.11.2022
    def get_questions(self, user_id):

        ft = "!SDg__b24DJUxcgTPKshKJML015yDk(YTbVB.Zsszyowk3Y2.12HhfA4*9j1UDSJw"
        questions = None

        try:
            questions = self.SITE.fetch(f"users/{user_id}/questions", fromdate=self.START_DATE, todate=self.END_DATE, sort="votes", order="desc", filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")
        
        return questions["items"]
        
    # TODO change date
    def get_answers(self, user_id):
        ft = "!D4oe0pk494TPqmPIxMhLQQhXC8w3TLywsgaR33Cqg58tJt6_I(i"
        answers = None

        try:
            answers = self.SITE.fetch(f"users/{user_id}/answers", sort="votes", order="desc", fromdate=self.START_DATE, todate=self.END_DATE, filter=ft)
        except StackAPIError as e:
            logging.ERROR(f"Error {e.error} when fetching from API: {e.message}")
    
        return answers["items"]
