import logging
import mysql
from mysql.connector import Error

class SQLManager:

    # TODO create different table for collectives
    def __init__(self, host_name, user_name, user_password, db_name):
        self._create_db_connection(host_name, user_name, user_password, db_name)
        
        self.create_users_table()
        self.create_questions_table()
        self.create_answers_table()

    def create_users_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS users (
            account_id INT PRIMARY KEY,
            user_id INT UNIQUE NOT NULL,
            user_type VARCHAR(20),
            age INT,
            location VARCHAR(255),  
            is_employee BOOL,
            display_name VARCHAR(255),
            accept_rate INT,
            reputation INT,
            view_count INT,
            question_count INT,
            answer_count INT,
            badge_counts_gold INT,
            badge_counts_silver INT,
            badge_counts_bronze INT,
            up_vote_count INT,
            down_vote_count INT,
            creation_date INT,
            last_access_date INT,
            last_modified_date INT,
            timed_penalty_date INT,
            collectives TEXT
        );
        """

        logging.info("Initializing users table.")
        self._execute_write_query(query)

    def create_answers_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS answers (
            answer_id INT PRIMARY KEY,
            owner INT,
            question_id INT, 
            body_markdown TEXT,
            awarded_bounty_amount INT,
            comment_count INT,
            up_vote_count INT,
            down_vote_count INT,
            upvoted BOOL,
            downvoted BOOL,
            score INT,
            accepted BOOL,
            is_accepted BOOL,
            creation_date INT,
            locked_date INT,
            community_owned_date INT,
            collectives TEXT,

            CONSTRAINT fk_answers_questions
            FOREIGN KEY (question_id) REFERENCES questions(question_id) ON DELETE CASCADE
        );
        """
        logging.info("Initializing answers table.")
        self._execute_write_query(query)
    
    def create_questions_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS questions (
            question_id INT PRIMARY KEY,
            accepted_answer_id INT,
            title TEXT,
            view_count INT,
            answer_count INT,
            comment_count INT,
            close_vote_count INT,
            delete_vote_count INT,
            up_vote_count INT,
            down_vote_count INT,
            downvoted BOOL,
            upvoted BOOL,
            favorite_count INT,
            reopen_vote_count INT,
            score INT,
            favorited BOOL,
            is_answered BOOL,
            closed_reason TEXT,
            body_markdown TEXT,
            migrated_from TEXT,
            migrated_to TEXT,
            bounty_amount INT,
            bounty_closes_date INT,
            community_owned_date INT,
            creation_date INT,
            closed_date INT,
            last_activity_date INT,
            last_edit_date INT,
            locked_date INT,
            protected_date INT,
            collectives TEXT
        );  
        """

        logging.info("Initializing questions table.")
        self._execute_write_query(query)

    def _create_db_connection(self, host_name, user_name, user_password, db_name):  
        self.connection = None
        try:
            self.connection = mysql.connector.connect( 
                host=host_name,
                user=user_name,
                passwd=user_password,
                database=db_name
            )
            logging.info(f"MySQL Database connection with {db_name} successful.")

        except Error as err:
            logging.error(f"Error when connecting with databank {db_name}: {err}.")

    def _execute_read_query(self, query, dict_data={}):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, dict_data)
            return cursor.fetchall()
        except Error as e:
            logging.error(f"Error when executing query {query} : {e}.")
            return None

    def _execute_write_query(self, query, dict_data={}):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, dict_data)
            self.connection.commit()
            
        except Error as err:
            logging.error(f"Error when excuting query {query} : {err}.")

x = SQLManager("localhost", "root" ,"password", "stackexchange")