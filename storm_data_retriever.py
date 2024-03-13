import warnings
import calendar
import datetime
import pandas as pd
from io import StringIO

import http.client
from urllib.request import urlretrieve
import requests

import time
import os


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
    preprocess_raw_storm_text_data():
        Clean the raw text response to fit the dataframe table
    get_storm_data():
        Decode the response from the GET request to a text file and return it as a pandas dataframe.
    """
    def __init__(self, start_date, end_date, state):
        """
        Constructs all the necessary attributes for the search_storm_data object.

        Parameters
        ----------
            start_date : datetime
                the data's start date (year, month, day) in datetime format e.g. datetime.datetime(year = 2000, month = 1, day = 1)
            end_date : datetime
                the data's end date (year, month, day) in datetime format e.g. datetime.datetime(year = 2000, month = 12, day = 31)
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

        self.start_date = start_date
        self.end_date = end_date

        begin_mm, begin_dd, begin_yyyy = self.start_date.strftime("%m"), self.start_date.strftime("%d"), self.start_date.strftime("%Y")
        end_mm, end_dd, end_yyyy = self.end_date.strftime("%m"), self.end_date.strftime("%d"), self.end_date.strftime("%Y")

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

        print(self.start_date.strftime("%B %d, %Y"))
        print("=============HTTP STATUS=============")
        print(f"HTTP STATUS {self.response.status_code}: {http.client.responses[self.response.status_code]}")
        print(f"URL: {self.response.url}\n")

        print("=============URL PARAMETERS=============")
        for k, v in self.query_parameters.items():
            print(f"{k} = {v}")

    def preprocess_raw_storm_text_data(self, text: str):
        """
        Preprocess the text by iterating each line of text. If the line is a full row, then immediately convert the line into dataframe and concatenate the line into the dataframe. If not, then multiple lines that consist of the beginning until the ending line is combined first.
        Parameters
        ----------
        text : str
            The text file obtained from NOAA database website

        Returns
        -------
        data : Dataframe
            the preprocessed text file already in dataframe format

        """
        txt_split = text.split("\n") # split the text into multiple lines
        columns = txt_split[0].split(",") # obtain columns
        txt_split = txt_split[1:] # obtain just the content and not the columns
        data = pd.DataFrame(columns = range(39))  # create empty dataframe with 39 columns

        # group the lines idx to identify whether the line is a complete row, incomplete row, or the end of a row
        full_line = []
        line_incomplete = []
        line_ending = []

        for i in range(len(txt_split)):
            line = txt_split[i].split(",")
            if line[0].isdigit() and line[-1].isdigit():  # check if line is a full row
                full_line.append(i)
            elif line[0].isdigit() and line[-1].isdigit() == False:  # check if line is incomplete
                line_incomplete.append(i)
            elif line[-1].isdigit():  # check if line is the ending
                line_ending.append(i)

        # obtain the full index of both complete and incomplete rows to be processed
        rows_index_lst = full_line + line_incomplete
        rows_index_lst.sort()

        for i in rows_index_lst:
            if i in full_line: # immediately convert line into dataframe since no preprocessing is needed
                line = txt_split[i].split(",")
                line = pd.DataFrame(line[:37] + [" ".join(line[37:-1])] + line[-1:]).T
                data = pd.concat([data, line]).reset_index(drop=True)

            elif i in line_incomplete: # preprocess the line by combining multiple lines to form the full rows
                ending_idx = line_ending[line_incomplete.index(i)]
                line = txt_split[i: ending_idx + 1]
                line = ",".join(line).split(",")
                line = pd.DataFrame(line[:37] + [" ".join(line[37:-1])] + line[-1:]).T
                data = pd.concat([data, line]).reset_index(drop=True)

        data.columns = columns
        return data


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
        response_text_file = self.response.content.decode('UTF-8')
        if len(response_text_file) > 0: # check if the raw text file is empty or not
            try:
                data = pd.read_csv(StringIO(response_text_file), sep=",")
                return data

            except:
                # There is a super annoying bug in the "EVENT_NARRATIVE" and "EPISODE_NARRATIVE" columns where if the text contains ',' then it adds extra columns due to comma separation
                # In such case, then I have to manually use string manipulation to fix the comma issues.
                print("Fixing mismatched number of columns issues\n.........")

                return self.preprocess_raw_storm_text_data(text = response_text_file)

        else:
            # returns empty pandas dataframe if the text file is empty
            return pd.DataFrame()


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
    get_daily_storm_data():
        Returns the daily storm events data.
    get_monthly_storm_data():
        Returns the monthly storm events data.
    get_monthly_storm_data_brute_force():
        Query monthly storm events data through daily requests. Use only if the regular monthly request does not work.
    """

    def __init__(self, year: int, state: str):
        """
        Constructs all the necessary attributes to obtain the annual storm events data.

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
        Loops through each month of the year to obtain and concatenate monthly data.
        Returns
        -------
        If everything goes smoothly
            data : pandas dataframe
                The queried annual storm events data
        Else
            False : bool
        """
        tic = time.perf_counter()

        print(f"Getting {self.state} data from {self.year}\n")
        get_periodical_data = get_periodical_storm_events_data(year = self.year, state = self.state)

        data = pd.DataFrame()

        for i in range(1, 13):  # loop through each month of the year
            monthly_data = self.get_monthly_storm_data(month = i)

            if type(monthly_data) == pd.DataFrame: # check if the returned content is a dataframe
                data = pd.concat([data, monthly_data])

            elif type(monthly_data) == int and monthly_data == 500: # otherwise, return False
                toc = time.perf_counter()
                print(f"\n{self.state} {self.year} data encountered query limit. Time passed: {((toc - tic) / 60):0.1f} minutes")

                return False

        toc = time.perf_counter()
        print(f"\nQueried {self.state} {self.year} data in {((toc - tic) / 60):0.1f} minutes")

        return data


    def get_monthly_storm_data(self, month: int):
        """
        Returns the monthly storm events data.
        Calls get_storm_data method to obtain the concatenated monthly data. Checks the daily HTTP status code in the process. Status code is always 100 in the beginning.

        Parameters
        ----------
        month : int
            The queried month

        Returns
        -------
        If data has less than 500 rows
            data : pandas dataframe
                The queried annual storm events data
        If data has 500 rows or more
            500 : int
        """
        m_range = calendar.monthrange(year = self.year, month = month) # get that month's beginning date and last date

        start_date = datetime.datetime(year = self.year, month = month, day = 1) # start date is always first day of the month
        end_date = datetime.datetime(year = self.year, month = month, day = m_range[1])

        state_fips = self.noaa_statefips[self.noaa_statefips["state_area"] == self.state]["statefips"].values[0]

        data = pd.DataFrame()

        print(f"...retrieving monthly {self.state} data from {start_date.strftime('%B %Y')}...")
        status_code = 100

        while status_code != 200: # check if database HTTP status is ready
            print(f"HTTP STATUS {status_code}: {http.client.responses[status_code]}")
            print("HTTP status code is not ready for query, retrying in 0.5 seconds...") # initial time delay is shorter for monthly query
            time.sleep(0.5)
            monthly_storm_data_request = search_storm_data(start_date = start_date, end_date = end_date, state = state_fips)
            status_code = monthly_storm_data_request.response.status_code

        print(f"HTTP STATUS {status_code}: {http.client.responses[status_code]}")

        try:
            data = pd.concat([data, monthly_storm_data_request.get_storm_data()])
            if data.shape[0] < 500:
                return data.reset_index(drop=True)

            elif data.shape[0] >= 500:  # gives warning if the data has 500 rows or more
                print("MONTHLY DATA HAS 500 ROWS OR MORE, RE-QUERYING USING DAILY DATA REQUEST")

                data = self.get_monthly_storm_data_brute_force(month = month, last_day = m_range[1])

                return data
        except:
            print("ERROR ENCOUNTERED FROM MONTHLY QUERYING THE NOAA DATABASE, RE-QUERYING USING DAILY DATA REQUEST")

            data = self.get_monthly_storm_data_brute_force(month = month, last_day = m_range[1])
            return data


    def get_monthly_storm_data_brute_force(self, month: int, last_day: int):
        """
        Query monthly storm events data through daily requests.
        Use only if there is an error querying storm events data by month as the NOAA website does not like multiple queries in a short period of time.

        Parameters
        ----------
        month : int
            The queried month
        last_day : int
            Last day of the month

        Returns
        -------
        If data has less than 500 rows
            data : pandas dataframe
                The queried annual storm events data
        If data has 500 rows or more
            500 : int
        """
        data = pd.DataFrame()
        for i in range(1, last_day + 1):  # loops the daily data 1 by one
            daily_data = self.get_daily_storm_data(month=month, day=i)

            if type(daily_data) == pd.DataFrame:  # check if the returned content is a dataframe
                data = pd.concat([data, daily_data])

            elif type(daily_data) == int and daily_data == 500:  # otherwise, return False
                print(f"\n{self.state} {self.year} daily data encountered query limit")

                return 500

        return data.reset_index(drop = True)


    def get_daily_storm_data(self, month : int, day : int):
        """
        Returns the daily storm events data.
        Calls get_storm_data method to obtain the concatenated daily data. Checks the daily HTTP status code in the process. Status code is always 100 in the beginning.

        Parameters
        ----------
        month : int
            The queried month of the day

        day : int
            The queried day

        Returns
        -------
        If data is less than 500 rows
            data : pandas dataframe
                The queried annual storm events data
        If data has 500 rows or more
            500 : int
        """
        date = datetime.datetime(year = self.year, month = month, day = day) # since this method queries daily data so the start date and end date are the same
        state_fips = self.noaa_statefips[self.noaa_statefips["state_area"] == self.state]["statefips"].values[0]

        data = pd.DataFrame()

        print(f"...retrieving daily {self.state} data from {date.strftime('%d %B %Y')}...")
        status_code = 100

        while status_code != 200: # check if database HTTP status is ready
            print(f"HTTP STATUS {status_code}: {http.client.responses[status_code]}")
            print("HTTP status code is not ready for query, retrying in 1 seconds...")
            time.sleep(1)
            daily_storm_data_request = search_storm_data(start_date = date, end_date = date, state=state_fips)
            status_code = daily_storm_data_request.response.status_code

        print(f"HTTP STATUS {status_code}: {http.client.responses[status_code]}")

        data = pd.concat([data, daily_storm_data_request.get_storm_data()])

        if data.shape[0] < 500:
            return data.reset_index(drop = True)

        elif data.shape[0] == 500: # gives warning if the data is exactly 500 rows
            warnings.warn(f'DAILY DATA HAS 500 ROWS')
            print("WARNING: DAILY DATA HAS 500 ROWS")

            return 500

        else:
            warnings.warn(f'DAILY DATA HAS MORE THAN 500 ROWS')
            print("WARNING: DAILY DATA HAS MORE THAN 500 ROWS")

            return 500


class save_storm_data:
    """
    This class saves the data with a given state and time in CSV format.
    WARNING: can take forever.

    Attributes
    ----------
    states: list[str]
        list of all states in the US

    Methods
    -------
    save_annual_storm_data():
        This method loops through annual storm events data from all states from a given list of year(s).
    save_annual_storm_data_multi_states():
        This method calls the get_periodical_storm_events_data class and queries the annual data to and saves it into a pre-deretmined folder.
    """
    def __init__(self):
        """
        Constructs all the necessary attributes to save storm events data.
        """
        # load an array of states from the reference file
        self.states = pd.read_json("reference_data/counties_list.json")["State"].unique()


    def save_annual_storm_data(self, year: int, state: str, folder = ".") -> None:
        """
        This method calls the get_periodical_storm_events_data class and queries the annual data to and saves it into a pre-deretmined folder.

        Parameters
        ----------
        year : int
        state : str
        folder : str, optional
            The folder to store the saved csv data. Saved in the main directory as default.

        Returns
        -------
        None
        """
        path = f"{folder}/{year}/"
        filename = f"{path}{state}_storm_events_{year}.csv"

        if not os.path.exists(path): # create folder if folder does not exist
            os.makedirs(path)

        get_periodical_data = get_periodical_storm_events_data(year = year, state = state)
        data = get_periodical_data.get_annual_storm_data()
        data.to_csv(filename, index = False)
        print(f"\nDownloaded {state} {year} data in {folder} folder")


    def save_annual_storm_data_multi_states(self, years: list[int], states = None, folder = ".") -> None:
        """
        This method loops through annual storm events data from all states from a given list of year(s).
        Then the annual data is saved in a specified folder. It also tells how long it takes to query the annual data from a given state.

        Parameters
        ----------
        years : list
            List all the year(s) to be queried.
            RECOMMENDATION: query 1 year at a time e.g. [2008] to prevent large files.
        states : list, optional
            List of states to be queried e.g. ["ALABAMA", "FLORIDA"], by default the method will query all states
        folder : str, optional
            The folder to store the saved csv data. Saved in the main directory as default.

        Returns
        -------
        None
        """
        if states == None:
            states = self.states

        time_taken_lst = []

        for i, year in enumerate(years):
            for j, state in enumerate(states):
                tic = time.perf_counter()

                print(f"Getting {state} ({j + 1}/ {len(states)}) data from {year} ({i + 1}/ {len(years)})\n")
                self.save_annual_storm_data(year = year, state = state, folder = folder)

                toc = time.perf_counter()

                time_taken_lst.append(toc - tic)

                average_time = sum(time_taken_lst) / len(time_taken_lst)
                estimated_time_seconds = (average_time * ((len(states) - j - 1) + (len(states) * (len(years) - i - 1))))
                print(f"\nEstimated time left before completion {(estimated_time_seconds / 60): 0.1f} minutes or {(estimated_time_seconds / 3600): 0.1f} hours\n")

            time.sleep(60) # wait 1 minute before querying the next year