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

Data Science Task for Data Intern Position at Koodoo.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# For display purposes
pd.set_option('display.width', 400)
pd.set_option("display.max.columns", None)


class AirbnbDataset:

    def __init__(self, file):
        self.data = pd.read_csv(file)
        self.correlation_matrix = None

    def get_dataset_shape(self):
        """
        :return: Number of lines and columns
        """
        return self.data.shape

    def get_head(self):
        """
        :return: First 10 lines of dataset
        """
        return self.data.head(10)

    def get_types(self):
        """
        We can see here that some features need some cleaning!
        Features without datatype (object) can't be analyzed properly.
        [host_since, host_is_superhost, State, neighbourhood_group, room_type, bathrooms] -> object
        :return: data types
        """
        # Some problems here!
        print(self.data.dtypes)

        # Host_since feature have 158255 str values and 29 float values, probably null/NaN!
        df = self.data.copy()
        obj_features = ["host_since", "host_is_superhost", "State", "neighbourhood_group", "room_type", "bathrooms"]
        for obj in obj_features:
            print(df[obj].apply(type).value_counts())

        # NaN values here, need to be fixed!
        for value in df["bathrooms"]:
            if isinstance(value, float):
                print(value)

        # Count null attr can be usefull!
        print(df.isnull().sum())

        return self.data.dtypes

    def encode_object_types(self, test_dataframe, feature):
        """
        Can be useful to encode object data types to further analysis
        ToDO:-> https://towardsdatascience.com/the-search-for-categorical-correlation-a1cf7f1888c9
        :param test_dataframe:
        :param feature:
        :return:
        """
        dummies = pd.get_dummies(test_dataframe[[feature]])
        res = pd.concat([test_dataframe, dummies], axis=1)
        return res

    def clean_dataset(self):
        """
        We could explore the data set a lot more if it related categorical variables using dummy variables,
        but it seems to me that at this moment this is not what we are looking for. then we do a little cleaning
        of NaN values ​​and boolean mapping.
        Future work -> https://towardsdatascience.com/the-search-for-categorical-correlation-a1cf7f1888c9
        :return:
        """
        values = {
            "host_is_superhost": False,
            "beds": 0,
            "bathrooms": 0,
            "bedrooms": 0,
            "reviews_per_month": 0
        }

        boolean = {
            't': True,
            'f': False
        }

        # Convert host_since to datetime64
        self.data["host_since"] = pd.to_datetime(self.data['host_since'], errors='coerce')
        # Convert host_is_superhost do boolean
        self.data["host_is_superhost"] = self.data["host_is_superhost"].replace(boolean)
        # Fill NaN Values
        self.data.fillna(value=values, inplace=True)
        # Null values feedback
        print(self.data.isnull().sum())

    def get_unique_values(self, column_name):
        """
        Get different values along a column
        :param column_name:
        :return: list of values
        """
        try:
            return self.data[column_name].unique()
        except KeyError:
            # Just for logging error handling
            print("Bad column name")

    def get_max_value(self, column_name):
        """
        Get the max value for a column
        :param column_name:
        :return: max value
        """
        try:
            return self.data[column_name].max()
        except KeyError:
            # Just for logging error handling
            print("Bad column name")

    def get_min_value_accommodation(self, column_name):
        try:
            # Remove accommodations with price = 0
            return self.data[self.data[column_name] != 0].min()
        except KeyError:
            # Just for logging error handling
            print("Bad column name")

    def get_correlation(self):
        """
        Get correlations between all numerical features with pearson method
        :return: correlation matrix
        """
        self.correlation_matrix = self.data.corr(method='pearson')
        return self.correlation_matrix

    def get_ordered_correlations(self):
        """
        Use the correlation matrix, unstack, sort the values and remove duplicates like diagonal 1's
        :return: ordered values
        """
        if self.correlation_matrix is None:
            self.get_correlation()
        return self.correlation_matrix.unstack().sort_values().drop_duplicates()

    def generate_correlation_plot(self):
        if self.correlation_matrix is None:
            self.get_correlation()
        """
        Pycharm do not support rendering HTML, matplotlib called !
        One way with matplotlib and another with seaborn!
        """
        figure, axis = plt.subplots(figsize=(10, 10))
        axis.matshow(self.correlation_matrix)
        plt.xticks(range(len(self.correlation_matrix)), self.correlation_matrix.columns, rotation='vertical')
        plt.yticks(range(len(self.correlation_matrix)), self.correlation_matrix.columns)
        plt.show()

        # Seaborn simpler plot!
        sns.heatmap(self.correlation_matrix,
                    xticklabels=self.correlation_matrix.columns.values,
                    yticklabels=self.correlation_matrix.columns.values)

    def generate_neighbourhood_group_plot(self):
        """
        Just for fun Plot
        :return:
        """
        plt.style.use('fivethirtyeight')
        plt.figure(figsize=(13, 7))
        plt.title("Neighbourhood Group")
        param = plt.pie(self.data.neighbourhood_group.value_counts(),
                    labels=self.data.neighbourhood_group.value_counts().index,
                    autopct='%1.1f%%',
                    startangle=180
                    )
        plt.show()

    def generate_price_distribution_by_state_plot(self):
        """
        https://stackoverflow.com/questions/38649501/labeling-boxplot-in-seaborn-with-median-value
        Best way to label the plot with median value!
        :return:
        """
        plt.style.use('classic')
        plt.figure(figsize=(13, 7))
        plt.title("Price Distribution by State")
        box_plot = sns.boxplot(y="price", x='State', data=self.data[self.data.price < 450])

        ax = box_plot.axes
        lines = ax.get_lines()
        categories = ax.get_xticks()

        for cat in categories:
            # every 4th line at the interval of 6 is median line
            # 0 -> p25 1 -> p75 2 -> lower whisker 3 -> upper whisker 4 -> p50 5 -> upper extreme value
            y = round(lines[4 + cat * 6].get_ydata()[0], 1)

            ax.text(
                cat,
                y,
                f'{y}',
                ha='center',
                va='center',
                fontweight='bold',
                size=10,
                color='white',
                bbox=dict(facecolor='#445A64'))

        box_plot.figure.tight_layout()
        plt.show()

    def generate_price_distribution_by_room_plot(self):
        """
        https://stackoverflow.com/questions/38649501/labeling-boxplot-in-seaborn-with-median-value
        Best way to label the plot with median value!
        :return:
        """
        plt.style.use('classic')
        plt.figure(figsize=(13, 7))
        plt.title("Price Distribution by Room Type")
        box_plot = sns.boxplot(y="price", x='room_type', data=self.data[self.data.price < 400])

        ax = box_plot.axes
        lines = ax.get_lines()
        categories = ax.get_xticks()

        for cat in categories:
            # every 4th line at the interval of 6 is median line
            # 0 -> p25 1 -> p75 2 -> lower whisker 3 -> upper whisker 4 -> p50 5 -> upper extreme value
            y = round(lines[4 + cat * 6].get_ydata()[0], 1)

            ax.text(
                cat,
                y,
                f'{y}',
                ha='center',
                va='center',
                fontweight='bold',
                size=10,
                color='white',
                bbox=dict(facecolor='#445A64'))

        box_plot.figure.tight_layout()
        plt.show()


if __name__ == '__main__':
    file_path = "/home/joaocps/Koodoo/Koodoo_Data_Intern/"
    file_name = "airbnb_dataset.csv"

    # Creation of object only one time to posterior analyze
    dataset = AirbnbDataset(file_path + file_name)

    # Get number of rows and columns - (158284, 16)
    print(dataset.get_dataset_shape())

    # Get first ten rows
    print(dataset.get_head())

    # Get column types
    print(dataset.get_types())

    # try clean
    print(dataset.clean_dataset())

    # Get unique values from column
    print(dataset.get_unique_values(column_name="bedrooms"))

    # Get max values from column
    print(dataset.get_max_value(column_name="price"))

    # Find the pairwise correlation of all columns in the dataframe
    print(dataset.get_correlation())

    # Get ordered correlations
    print(dataset.get_ordered_correlations())

    # Generate plot
    print(dataset.generate_correlation_plot())

    # Neighbourhood group plot
    print(dataset.generate_neighbourhood_group_plot())

    # Generate price distribution by room plot
    print(dataset.generate_price_distribution_by_room_plot())

    # Generate price distribution by state plot
    print(dataset.generate_price_distribution_by_state_plot())
