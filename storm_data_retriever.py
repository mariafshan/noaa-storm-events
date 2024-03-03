import warnings
import datetime
import pandas as pd
from io import StringIO

import http.client
from urllib.request import urlretrieve
import requests

import time


class search_storm_data:
    """
    This class is specifically written to query daily data from ncdc noaa storm events.
    This class takes the data parameters to query storm events data from NCDC NOAA website
    NCDC NOAA website: https://www.ncdc.noaa.gov/stormevents/

    Attributes
    ----------
    date : datetime
        the data's date (year, month, day) in datetime format e.g. datetime.datetime(year = 2022, month = 1, day = 1)
    state : str
        US State in FIPS%2STATENAME format e.g. 1%2CALABAMA

    Methods
    -------
    check_url_content():
        Print the content of the retrieved url.
    check_http_status():
        Print the HTTP status of the GET request.
    get_storm_data():
        Decode the response from the GET request to a text file and return it as a pandas dataframe.
    """

    def __init__(self, date, state):
        """
        Constructs all the necessary attributes for the search_storm_data object.

        Parameters
        ----------
            date : datetime
                the data's date (year, month, day) in datetime format e.g. datetime.datetime(year = YEAR, month = 1, day = 1)
            state : str
                US State in FIPS%2STATENAME format e.g. 1%2CALABAMA
            url : str
                https://www.ncdc.noaa.gov/stormevents/csv
            query_parameters : dict
                the query parameters necessary to query the right data.
                eventType, beginDate_mm, beginDate_dd, beginDate_yyyy, endDate_mm, endDate_dd, endDate_yyyy, county, hailfilter, tornfilter, windfilter, sort, submitbutton, statefips
            payload_str : str
                the query parameters in full form
            response : text
                The storm data in raw text format obtained through the HTTP GET
        """

        self.date = date
        begin_mm, begin_dd, begin_yyyy = end_mm, end_dd, end_yyyy = self.date.strftime("%m"), self.date.strftime("%d"), self.date.strftime("%Y")

        self.url = "https://www.ncdc.noaa.gov/stormevents/csv"

        self.query_parameters = {"eventType": "ALL", "beginDate_mm": begin_mm, "beginDate_dd": begin_dd,
                                 "beginDate_yyyy": begin_yyyy, "endDate_mm": end_mm, "endDate_dd": end_dd,
                                 "endDate_yyyy": end_yyyy, "county": "ALL", "hailfilter": "0.00", "tornfilter": "0",
                                 "windfilter": "000", "sort": "DT", "submitbutton": "Search", "statefips": state}
        self.payload_str = "&".join("%s=%s" % (k, v) for k, v in self.query_parameters.items())

        self.response = requests.get(self.url, params=self.payload_str)


    def check_url_content(self) -> None:
        """
        Print the content of the retrieved url.

        Returns
        -------
        None
        """

        endpoint = self.url + "?" + self.payload_str

        path, headers = urlretrieve(endpoint)
        for name, value in headers.items():
            print(f"{name}: {value}")


    def check_http_status(self) -> None:
        """
        Print the HTTP status of the GET request.

        Returns
        -------
        None
        """

        print(date.strftime("%B %d, %Y"))
        print("=============HTTP STATUS=============")
        print(f"HTTP STATUS {self.response.status_code}: {http.client.responses[200]}")
        print(f"URL: {self.response.url}\n")

        print("=============URL PARAMETERS=============")
        for k, v in self.query_parameters.items():
            print(f"{k} = {v}")


    def get_storm_data(self):
        """
        Decode the response from the GET request to a text file and return it as a pandas dataframe.
        Will give a warning the total number of rows is 500.

        Returns
        -------
        If the text file is not empty:
            data : Pandas Dataframe
                The storm event data
        If the text file is empty:
            date : Pandas Dataframe
                Empty pandas dataframe
        """

        if self.response.status_code == 200:
            # returns pandas dataframe if the response status is OK

            response_text_file = self.response.content.decode('UTF-8')
            if len(response_text_file) > 0: # check if the raw text file is empty or not
                data = pd.read_csv(StringIO(response_text_file), sep=",")
                if data.shape[0] == 500: # gives warning if the data is exactly 500 rows
                    warnings.warn(f'{self.date} DATA IS EXACTLY 500 ROWS')
                return data
            else:
                # returns empty pandas dataframe if the text file is empty
                return pd.DataFrame()

        else:
            # check response status if there is an error in data query
            print(self.check_http_status())


class get_periodical_storm_events_data:
    """
    This class gets the periodical storm events data by looping the get_storm_data method to obtain the concatenated daily data.

    Attributes
    ----------
    year : int
        the data's year
    state : str
        US State in FIPS%2STATENAME format e.g. 1%2CALABAMA

    Methods
    -------
    get_annual_storm_data():
        Returns the annual storm events data.
    save_annual_storm_data():
        Query the annual storm data and save the annual storm data in CSV format.
    """

    def __init__(self, year: int, state: str):
        """
        Constructs all the necessary attributes to obtain the annual storm evebts data.

        Parameters
        ----------
        year : int
            the data's year
        state : str
            US State in FIPS%2STATENAME format e.g. 1%2CALABAMA
        """
        self.year = year
        self.state = state
        self.noaa_statefips = pd.read_json("reference_data/noaa_statefips.json")


    def get_annual_storm_data(self):
        """
        Returns the annual storm events data.
        Loops the get_storm_data method to obtain the concatenated daily data.

        Returns
        -------
        data : pandas dataframe
            The queried annual storm events data
        """
        date = datetime.datetime(year = self.year, month = 1, day = 1)
        state_fips = self.noaa_statefips[self.noaa_statefips["state_area"] == self.state]["statefips"].values[0]

        data = pd.DataFrame()
        pre_month = 0

        while (date < datetime.datetime(year = self.year, month = 12, day = 31)):
            # loop and concatenate daily data until the annual data is obtained
            if date.strftime("%m") != pre_month:
                print(f"...retrieving {self.state} data from {date.strftime('%B %Y')}...")
                pre_month = date.strftime("%m")

            data = pd.concat([data, search_storm_data(date = date, state = state_fips).get_storm_data()])
            date += datetime.timedelta(days=1)

        return data.reset_index(drop = True)

    def save_annual_storm_data(self, folder = "./") -> None:
        """
        Query the annual storm data and save the annual storm data in CSV format.

        Parameters
        ----------
        folder : str, optional
            The folder to store the saved csv data. Saved in the main directory as default.
        Returns
        -------
        None
        """
        filename = f"{folder}{self.state}_storm_events_{self.year}.csv"

        tic = time.perf_counter()

        data = self.get_annual_storm_data()
        data.to_csv(filename, index = False)

        toc = time.perf_counter()
        print(f"\nDownloaded the {self.state} {self.year} data in {((toc - tic) / 60):0.1f} minutes\n")

class get_all_states_data:
    """
    This class gets the periodical storm events data for all States.
    WARNING: can take forever.

    Methods
    -------
    get_annual data():
        This method loops through annual storm events data from all states from a given list of year(s).
    """
    def get_annual_data(self, years: list[int], folder = "./"):
        """
        This method loops through annual storm events data from all states from a given list of year(s).
        Then the annual data is saved in a specified folder. It also tells how long it takes to query the annual data from a given state.

        Parameters
        ----------
        years : list
            List all the year(s) to be queried.
            RECOMMENDATION: query 1 year at a time e.g. [2008] to prevent large files.
        folder : str, optional
            The folder to store the saved csv data. Saved in the main directory as default.

        Returns
        -------
        None
        """
        # load an array of states from the reference file
        states = pd.read_json("reference_data/counties_list.json")["State"].unique()

        for year in years:
            for i, state in enumerate(states):
                print(f"Getting {state} ({i + 1}/ {len(states)}) data from {year}\n")
                get_periodical_storm_events_data(year = year, state = state).save_annual_storm_data(folder = folder)
