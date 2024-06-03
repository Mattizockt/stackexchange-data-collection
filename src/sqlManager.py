import logging
import mysql
from mysql.connector import Error

class SQLManager:

    # TODO create different table for collectives
    def __init__(self, host_name, user_name, user_password, db_name):
        self._create_db_connection(host_name, user_name, user_password, db_name)
        
        self.create_users_table(user_table_name="users_control")
        self.create_users_table(user_table_name="users_treatment")
        self.create_questions_table()
        self.create_answers_table()

    def create_users_table(self, user_table_name):
        query = """
        CREATE TABLE IF NOT EXISTS {user_table_name} (
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
        """.format(user_table_name=user_table_name)

        logging.info(f"Initializing {user_table_name} table.")
        self._execute_write_query(query)

    # filter: !)E-ko157Q3L6uLnKttPbTnIxVK9xpMlC)7tq69oEWVe1FOEmL
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
            collectives TEXT
        );
        """
        logging.info("Initializing answers table.")
        self._execute_write_query(query)
    
    def create_questions_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS questions (
            question_id INT PRIMARY KEY,
            accepted_answer_id INT,
            owner INT,
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

    def insert_into_users_table(self, user_file):
        if not user_file.get("location"): return
        
        user_table_name = self._determine_user_table_name(user_file["location"])
        if self.get_users_table_count(user_table_name) > 1000: 
            logging.info(f"1000 entries for {user_table_name} reached.")
            return   
        
        data_to_insert = {
            "account_id": user_file.get("account_id", None),
            "user_id": user_file.get("user_id", None),
            "user_type": user_file.get("user_type", None),
            "age": user_file.get("age", None),
            "location": user_file.get("location", None),
            "is_employee": user_file.get("is_employee", None),
            "display_name": user_file.get("display_name", None),
            "accept_rate": user_file.get("accept_rate", None),
            "reputation": user_file.get("reputation", None),
            "view_count": user_file.get("view_count", None),
            "question_count": user_file.get("question_count", None),
            "answer_count": user_file.get("answer_count", None),
            "badge_counts_gold": user_file.get("badge   _counts", {}).get("gold", None),
            "badge_counts_silver": user_file.get("badge_counts", {}).get("silver", None),
            "badge_counts_bronze": user_file.get("badge_counts", {}).get("bronze", None),
            "up_vote_count": user_file.get("up_vote_count", None),
            "down_vote_count": user_file.get("down_vote_count", None),
            "creation_date": user_file.get("creation_date", None),
            "last_access_date": user_file.get("last_access_date", None),
            "last_modified_date": user_file.get("last_modified_date", None),
            "timed_penalty_date": user_file.get("timed_penalty_date", None),
            "collectives": self._collective_to_string(user_file.get("collectives", None)) # CHANGE
        }

        query = """
            INSERT INTO {user_table_name} (
                account_id, user_id, user_type, age, location, is_employee, display_name,
                accept_rate, reputation, view_count, question_count, answer_count,
                badge_counts_gold, badge_counts_silver, badge_counts_bronze,
                up_vote_count, down_vote_count, creation_date, last_access_date,
                last_modified_date, timed_penalty_date, collectives
            )
            VALUES (
                %(account_id)s, %(user_id)s, %(user_type)s, %(age)s, %(location)s,
                %(is_employee)s, %(display_name)s, %(accept_rate)s, %(reputation)s,
                %(view_count)s, %(question_count)s, %(answer_count)s,
                %(badge_counts_gold)s, %(badge_counts_silver)s, %(badge_counts_bronze)s,
                %(up_vote_count)s, %(down_vote_count)s, %(creation_date)s,
                %(last_access_date)s, %(last_modified_date)s, %(timed_penalty_date)s,
                %(collectives)s
            )
            ON DUPLICATE KEY UPDATE
                account_id = VALUES(account_id),
                user_id = VALUES(user_id),
                user_type = VALUES(user_type),
                age = VALUES(age),
                location = VALUES(location),
                is_employee = VALUES(is_employee),
                display_name = VALUES(display_name),
                accept_rate = VALUES(accept_rate),
                reputation = VALUES(reputation),
                view_count = VALUES(view_count),
                question_count = VALUES(question_count),
                answer_count = VALUES(answer_count),
                badge_counts_gold = VALUES(badge_counts_gold),
                badge_counts_silver = VALUES(badge_counts_silver),
                badge_counts_bronze = VALUES(badge_counts_bronze),
                up_vote_count = VALUES(up_vote_count),
                down_vote_count = VALUES(down_vote_count),
                creation_date = VALUES(creation_date),
                last_access_date = VALUES(last_access_date),
                last_modified_date = VALUES(last_modified_date),
                timed_penalty_date = VALUES(timed_penalty_date),
                collectives = VALUES(collectives);
        """.format(user_table_name=user_table_name)

        logging.info(f"Inserting into {user_table_name} table account {user_file.get('account_id')}.")
        self._execute_write_query(query, data_to_insert)

    # TODO add, whether accepted
    def insert_into_questions_table(self, question_file):
        data_to_insert = {
            "question_id": question_file.get("question_id", None),
            "accepted_answer_id": question_file.get("accepted_answer_id", None),
            "owner": question_file.get("owner", {}).get("user_id", None),
            "title": question_file.get("title", None),
            "view_count": question_file.get("view_count", None),
            "answer_count": question_file.get("answer_count", None),
            "comment_count": question_file.get("comment_count", None),
            "close_vote_count": question_file.get("close_vote_count", None),
            "delete_vote_count": question_file.get("delete_vote_count", None),
            "up_vote_count": question_file.get("up_vote_count", None),
            "down_vote_count": question_file.get("down_vote_count", None),
            "downvoted": question_file.get("downvoted", None),
            "upvoted": question_file.get("upvoted", None),
            "favorite_count": question_file.get("favorite_count", None),
            "reopen_vote_count": question_file.get("reopen_vote_count", None),
            "score": question_file.get("score", None),
            "favorited": question_file.get("favorited", None),
            "is_answered": question_file.get("is_answered", None),
            "closed_reason": question_file.get("closed_reason", None),
            "body_markdown": question_file.get("body_markdown", None),
            "migrated_from": question_file.get("migrated_from", {}).get("other_site", None),
            "migrated_to": question_file.get("migrated_to", None).get("other_site", None),
            "bounty_amount": question_file.get("bounty_amount", None),
            "bounty_closes_date": question_file.get("bounty_closes_date", None),
            "community_owned_date": question_file.get("community_owned_date", None),
            "creation_date": question_file.get("creation_date", None),
            "closed_date": question_file.get("closed_date", None),
            "last_activity_date": question_file.get("last_activity_date", None),
            "last_edit_date": question_file.get("last_edit_date", None),
            "locked_date": question_file.get("locked_date", None),
            "protected_date": question_file.get("protected_date", None),
            "collectives": self._collective_to_string(question_file.get("collectives", None))
        }

        query = """
            INSERT INTO questions (
                question_id, accepted_answer_id, owner, title, view_count, answer_count,
                comment_count, close_vote_count, delete_vote_count, up_vote_count,
                down_vote_count, downvoted, upvoted, favorite_count, reopen_vote_count,
                score, favorited, is_answered, closed_reason, body_markdown,
                migrated_from, migrated_to, bounty_amount, bounty_closes_date,
                community_owned_date, creation_date, closed_date, last_activity_date,
                last_edit_date, locked_date, protected_date, collectives
            )
            VALUES (
                %(question_id)s, %(accepted_answer_id)s, %(owner)s, %(title)s, %(view_count)s,
                %(answer_count)s, %(comment_count)s, %(close_vote_count)s,
                %(delete_vote_count)s, %(up_vote_count)s, %(down_vote_count)s,
                %(downvoted)s, %(upvoted)s, %(favorite_count)s, %(reopen_vote_count)s,
                %(score)s, %(favorited)s, %(is_answered)s, %(closed_reason)s,
                %(body_markdown)s, %(migrated_from)s, %(migrated_to)s,
                %(bounty_amount)s, %(bounty_closes_date)s, %(community_owned_date)s,
                %(creation_date)s, %(closed_date)s, %(last_activity_date)s,
                %(last_edit_date)s, %(locked_date)s, %(protected_date)s, %(collectives)s
            )
            ON DUPLICATE KEY UPDATE
                question_id = VALUES(question_id),
                accepted_answer_id = VALUES(accepted_answer_id),
                owner = VALUES(owner),
                title = VALUES(title),
                view_count = VALUES(view_count),
                answer_count = VALUES(answer_count),
                comment_count = VALUES(comment_count),
                close_vote_count = VALUES(close_vote_count),
                delete_vote_count = VALUES(delete_vote_count),
                up_vote_count = VALUES(up_vote_count),
                down_vote_count = VALUES(down_vote_count),
                downvoted = VALUES(downvoted),
                upvoted = VALUES(upvoted),
                favorite_count = VALUES(favorite_count),
                reopen_vote_count = VALUES(reopen_vote_count),
                score = VALUES(score),
                favorited = VALUES(favorited),
                is_answered = VALUES(is_answered),
                closed_reason = VALUES(closed_reason),
                body_markdown = VALUES(body_markdown),
                migrated_from = VALUES(migrated_from),
                migrated_to = VALUES(migrated_to),
                bounty_amount = VALUES(bounty_amount),
                bounty_closes_date = VALUES(bounty_closes_date),
                community_owned_date = VALUES(community_owned_date),
                creation_date = VALUES(creation_date),
                closed_date = VALUES(closed_date),
                last_activity_date = VALUES(last_activity_date),
                last_edit_date = VALUES(last_edit_date),
                locked_date = VALUES(locked_date),
                protected_date = VALUES(protected_date),
                collectives = VALUES(collectives);
        """
        
        logging.info(f"Inserting into questions table.")
        self._execute_write_query(query, data_to_insert)
        
    def insert_into_answers_table(self, answer_file):
        data_to_insert = {
            "answer_id": answer_file.get("answer_id", None),
            "owner": answer_file.get("owner", {}).get("user_id", None),
            "question_id": answer_file.get("question_id", None),
            "body_markdown": answer_file.get("body_markdown", None),
            "awarded_bounty_amount": answer_file.get("awarded_bounty_amount", None),
            "comment_count": answer_file.get("comment_count", None),
            "up_vote_count": answer_file.get("up_vote_count", None),
            "down_vote_count": answer_file.get("down_vote_count", None),
            "upvoted": answer_file.get("upvoted", None),
            "downvoted": answer_file.get("downvoted", None),
            "score": answer_file.get("score", None),
            "accepted": answer_file.get("accepted", None),
            "is_accepted": answer_file.get("is_accepted", None),
            "creation_date": answer_file.get("creation_date", None),
            "locked_date": answer_file.get("locked_date", None),
            "community_owned_date": answer_file.get("community_owned_date", None),
            "collectives": self._collective_to_string(answer_file.get("collectives", None))
        }

        query = """
            INSERT INTO answers (
                answer_id, owner, question_id, body_markdown, awarded_bounty_amount,
                comment_count, up_vote_count, down_vote_count, upvoted, downvoted, score,
                accepted, is_accepted, creation_date, locked_date, community_owned_date,
                collectives
            )
            VALUES (
                %(answer_id)s, %(owner)s, %(question_id)s, %(body_markdown)s,
                %(awarded_bounty_amount)s, %(comment_count)s, %(up_vote_count)s,
                %(down_vote_count)s, %(upvoted)s, %(downvoted)s, %(score)s, %(accepted)s,
                %(is_accepted)s, %(creation_date)s, %(locked_date)s,
                %(community_owned_date)s, %(collectives)s
            )
            ON DUPLICATE KEY UPDATE
                answer_id = VALUES(answer_id),
                owner = VALUES(owner),
                question_id = VALUES(question_id),
                body_markdown = VALUES(body_markdown),
                awarded_bounty_amount = VALUES(awarded_bounty_amount),
                comment_count = VALUES(comment_count),
                up_vote_count = VALUES(up_vote_count),
                down_vote_count = VALUES(down_vote_count),
                upvoted = VALUES(upvoted),
                downvoted = VALUES(downvoted),
                score = VALUES(score),
                accepted = VALUES(accepted),
                is_accepted = VALUES(is_accepted),
                creation_date = VALUES(creation_date),
                locked_date = VALUES(locked_date),
                community_owned_date = VALUES(community_owned_date),
                collectives = VALUES(collectives);
        """
        logging.info(f"Inserting into answers table.")
        self._execute_write_query(query, data_to_insert)

    def get_inserted_users(self, user_table_name):
        query = "SELECT user_id FROM {user_table_name}".format(user_table_name=user_table_name)
        res = self._execute_read_query(query)
        
        return [i[0] for i in res]
    
    def get_users_table_count(self, user_table_name):
        query = f"""SELECT COUNT(*) FROM {user_table_name}"""
        res = self._execute_read_query(query)

        return res[0][0]
    
    def _determine_user_table_name(self, input_string):
        countries_of_interest = ["russia", "venezuela", "china"]
        input_string_lower = input_string.lower()  # Convert to lowercase for case-insensitive comparison
        
        if any(country in input_string_lower for country in countries_of_interest):
            return "users_treatment"
        else:
            return "users_control"


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

    # TODO test function
    def _collective_to_string(self, cvs):
        if not cvs: return None

        cv_names = []
        for cv in cvs:
            cv_names.append(cv.get("collective", {}).get("name", ""))
        
        return ",".join(cv_names)

x = SQLManager("localhost", "root" ,"password", "stackexchange")