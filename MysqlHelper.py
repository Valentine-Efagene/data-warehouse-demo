from __future__ import print_function
from typing import List, Tuple

import mysql.connector
from mysql.connector import Error
import pandas as pd


class MysqlHelper:
    def __init__(self, host: str, user: str, password: str, database: str):
        """New instance of the MysqlHelper class

        Args:
            host {str}: Hostname
            user {str}: Username
            password {str}: Password
            database {str}: Database name
        """
        self._host = host
        self._user = user
        self._password = password
        self._database = database

        self.connection = mysql.connector.connect(
            host=self._host, user=self._user, password=self._password, database=self._database)
        self.cursor = self.connection.cursor()

    @staticmethod
    def createDatabase(host: str, user: str, password: str, database: str):
        """Static method to create a database

        Args:
            host {str}: Hostname
            user {str}: Username
            password {str}: Password
            database {str}: Database name
        """
        connection = mysql.connector.connect(
            host=host, user=user, password=password)
        cursor = connection.cursor()

        cursor.execute(f'DROP DATABASE IF EXISTS `{database}`;')
        connection.commit()
        cursor.execute(f'CREATE DATABASE `{database}`;')
        connection.commit()
        cursor.close()
        connection.close()

    @staticmethod
    def dropDatabase(host, user, password, database):
        """Static method to drop a database

        Args:
            host {str}: Hostname
            user {str}: Username
            password {str}: Password
            database {str}: Database name
        """
        connection = mysql.connector.connect(
            host=host, user=user, password=password)
        cursor = connection.cursor()

        cursor.execute(f'DROP DATABASE IF EXISTS `{database}`;')
        connection.commit()
        cursor.close()
        connection.close()

    def open(self):
        """ 
        Method to reopen a closed connection and cursor
        """
        self.connection = mysql.connector.connect(
            host=self._host, user=self._user, password=self._password, database=self._database)
        self.cursor = self.connection.cursor()

    def query(self, queryString: str, values: Tuple = None, fetchCount: int = -1):
        """
        Fetch data from a database

        Args:
            queryString {str}: Database query as a string
            values {?Tuple}: Tuple containing values to insert into a query at the placeholders. Defaults to None.
            fetchCount (?int): Number of rows to fetch. -1 means to fetch all. Defaults to -1.

        Returns:
            rows {List}: Returned MySQL rows
        """
        result = None

        try:
            if (values):
                self.cursor.execute(queryString, values)
            else:
                self.cursor.execute(queryString)

            if (fetchCount < 1):
                result = self.cursor.fetchall()
            elif (fetchCount == 1):
                result = self.cursor.fetchone()
            else:
                result = self.cursor.fetchmany(fetchCount)

            return result
        except Error as err:
            print(f"Error: '{err}'")

    def mutate(self, queryString: str, values: Tuple = None):
        """
        Execute a database mutation

        Args:
            queryString {str}: Database query as a string
            values {?Tuple}: Tuple containing values to insert into a query at the placeholders. Defaults to None.
        """
        try:
            if (values):
                self.cursor.execute(queryString, values)
            else:
                self.cursor.execute(queryString)

            self.connection.commit()
            print("Mutation successful")
        except Error as err:
            print(f"Error: '{err}'")

    def rowsToPDTable(self, rows: List, columns: List):
        """
        Convert the rows result of a query to a pandas table

        Args:
            rows {List}: Returned MySQL rows
            columns {List}: List of the column names for the pandas DataFrame

        Returns:
            Pandas.DataFrame: Pandas DataFrame
        """
        if rows == None:
            return None

        from_db = []

        for row in rows:
            from_db.append(list(row))

        return pd.DataFrame(from_db, columns=columns)

    def rowsToList(self, rows) -> List:
        """
        Convert 

        Args:
            rows {List}: Returned MySQL rows

        Returns:
            List: _description_
        """
        if rows == None:
            return None

        from_db = []

        for row in rows:
            from_db.append(list(row))

        return from_db

    def close(self):
        """
        Close connection and cursor
        """
        self.cursor.close()
        self.connection.close()

    def __del__(self):
        """
        Class destructor.
        Close connection and cursor
        """
        self.cursor.close()
        self.connection.close()
