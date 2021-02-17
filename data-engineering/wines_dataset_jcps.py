"""
Copyright (C) 2021 João Santos

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

João Carlos Pinto Santos
jcps@ua.pt

Data Engineering Challenge for Data Intern Position at Koodoo.
"""

import pandas as pd
import sqlite3

from sqlite3 import Error


class WinesDataset:
    def __init__(self, file):
        self.data = pd.read_csv(file)
        self.conn = None

    def create_db_connection(self, db_name):
        """
        Create a database connection to a SQLite database.
        :param db_name:
        :return:
        """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_name)
            print(sqlite3.version)
        except Error as e:
            print(e)

    def get_csv_data_types(self):
        """
        Get Wines data types to table creation.
        Only the variable "points" appears with an int64 datatype, the rest are represented as strings.
        :return:
        """

        self.data.Price = self.data['Price'].str.replace("$", "")
        self.data.Price = self.data['Price'].str.replace(",", "")

        print(pd.to_numeric(self.data['Price'], errors='raise').value_counts())

        return self.data.dtypes

    def create_db_table(self, table):
        """
        Create table.
        :return:
        """
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                c.execute(table)
            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def load_csv_into_table(self, table_name):
        if self.conn is not None:
            try:
                self.data.to_sql(table_name, self.conn, if_exists='append', index=False)
            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def select_all_wines(self):
        """
        Query all rows in the staging_wines table.
        :return:
        """
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                c.execute("SELECT * FROM staging_wines")
                rows = c.fetchall()
                for row in rows:
                    print(row)
            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def populate_dim_tables(self):
        """
        Populate dim tables with staging_wines values
        :return:
        """
        if self.conn is not None:
            try:
                c = self.conn.cursor()

                # Insert distinct winery names into dimension table (pk auto-increment) text->text
                c.execute("SELECT DISTINCT winery from staging_wines")
                rows = c.fetchall()
                c.executemany("INSERT INTO dimwinery (winery_name) VALUES (?)", rows)
                self.conn.commit()

                # Insert distinct variety names into dimension table (pk auto-increment) text->text
                c.execute("SELECT DISTINCT variety from staging_wines")
                rows = c.fetchall()
                c.executemany("INSERT INTO dimvariety (variety) VALUES (?)", rows)
                self.conn.commit()

                # Insert distinct (country,province,county) into dimension table (pk auto-increment) text->text
                c.execute("SELECT DISTINCT country, province, county from staging_wines")
                rows = c.fetchall()
                c.executemany("INSERT INTO dimgeography (country,province,county) VALUES (?,?,?)", rows)
                self.conn.commit()

            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def populate_fact_table(self):
        """
        Add fk to staging table, clean data and populate FactWine
        Last process, populate the fact table with all values, correct data types and FK! :)
        :return:
        """
        if self.conn is not None:
            try:
                c = self.conn.cursor()

                c.execute("INSERT INTO factwine("
                          "title,"
                          "winery_id,"
                          "geography_id,"
                          "variety_id,"
                          "points,"
                          "price,"
                          "vintage)"
                          "SELECT staging_wines.title, "
                          "dimvariety.variety_id, "
                          "dimgeography.geography_id, "
                          "dimwinery.winery_id, "
                          "staging_wines.points, "
                          "staging_wines.price, "
                          "staging_wines.vintage "
                          "FROM staging_wines "
                          "JOIN dimvariety ON staging_wines.variety == dimvariety.variety "
                          "JOIN dimgeography ON (staging_wines.country,staging_wines.province,staging_wines.county) == "
                          "(dimgeography.country,dimgeography.province,dimgeography.county) "
                          "JOIN dimwinery ON staging_wines.winery == dimwinery.winery_name")

                self.conn.commit()

            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def count_total_table_rows_sw(self):
        """
        Count all rows from table staging_wines
        :return:
        """
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                total = c.execute("SELECT Count(*) FROM staging_wines")
                return total.fetchone()
            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def get_price_info(self):
        """
        Get average price of a bottle of wine.
        The price datatype is TEXT, then we need to clean the data to perform de avg operation.
        Dataset cleaned !
        The process can be done directly into sql into CAST operator.
        Process the data after query them seems easier and more understandable.
        :return:
        """
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                prices = c.execute("SELECT price FROM staging_wines WHERE price IS NOT NULL").fetchall()
                total = 0
                for x in prices:
                    total += x[0]

                print("Average Price = ", total / len(prices))
                print("Price of most expensive wine = ", max(prices))

            except Error as e:
                print(e)
        else:
            print("Connection to database refused!")

    def close_db_connection(self):
        """
        Close connection to database.
        :return:
        """
        if self.conn:
            self.conn.close()


if __name__ == '__main__':
    file_path = "/home/joaocps/Koodoo/Koodoo_Data_Intern/"
    file_name = "Wines.csv"

    challenge = WinesDataset(file_path + file_name)

    challenge.create_db_connection("Wine.db")

    # TASK 1
    print(challenge.get_csv_data_types())

    # staging_wines table creation with correct data types
    sql_create_staging_wines_table = """ CREATE TABLE IF NOT EXISTS staging_wines (
    vintage TEXT,
    country TEXT,
    county TEXT,
    designation TEXT,
    points INTEGER,
    price REAL,
    province TEXT,
    title TEXT,
    variety TEXT,
    winery TEXT);
    """

    challenge.create_db_table(sql_create_staging_wines_table)

    # Load the csv into staging_wines table
    challenge.load_csv_into_table("staging_wines")

    # View all rows in staging_wines table
    challenge.select_all_wines()

    # Number of rows in staging_wines table
    print(challenge.count_total_table_rows_sw())

    # Average price of a bottle of wine.
    print(challenge.get_price_info())

    # TASK 2 - Create tables with relationships

    sql_create_dim_winery_table = """CREATE TABLE IF NOT EXISTS dimwinery (
    winery_id INTEGER PRIMARY KEY,
    winery_name TEXT)
    """

    sql_create_dim_geography_table = """CREATE TABLE IF NOT EXISTS dimgeography (
    geography_id INTEGER PRIMARY KEY,
    country TEXT,
    province TEXT,
    county TEXT)
    """

    sql_create_dim_variety_table = """CREATE TABLE IF NOT EXISTS dimvariety (
    variety_id INTEGER PRIMARY KEY,
    variety TEXT)
    """

    # MANY - TO - ONE relationships, one wine have one winery but one winery can have many wines
    sql_create_fact_wine_table = """CREATE TABLE IF NOT EXISTS factwine (
    wine_id INTEGER PRIMARY KEY,
    title TEXT,
    winery_id INTEGER,
    geography_id INTEGER,
    variety_id INTEGER,
    points INTEGER,
    price REAL,
    vintage TEXT,
    FOREIGN KEY (winery_id) REFERENCES dimwinery (winery_id),
    FOREIGN KEY (geography_id) REFERENCES dimgeography (geography_id),
    FOREIGN KEY (variety_id) REFERENCES dimvariety (variety_id)
    )
    """

    challenge.create_db_table(sql_create_dim_winery_table)
    challenge.create_db_table(sql_create_dim_geography_table)
    challenge.create_db_table(sql_create_dim_variety_table)
    challenge.create_db_table(sql_create_fact_wine_table)

    # The dim tables needs to be populated first
    challenge.populate_dim_tables()

    # Add fk references to staging table
    challenge.populate_fact_table()

