# Query and Transform Storm Events Data from National Oceanic and Atmospheric Administration
Data Source: https://www.ncdc.noaa.gov/stormevents/

Motivation: I had difficulties trying to obtain the API for NOAA's storm events data, so I created my own.

## Problem Statement
Conventionally, to obtain storm events data anyone can just go to the [NCDC NOAA's website](https://www.ncdc.noaa.gov/stormevents/) and manually specify the parameters. However, this poses several problems:

<ul>
    <li>
        <h4>The website limits each query to only 500 rows per query </h4>
        This is a problem especially if we need to query annual data as the data can can consist more than 500 rows. Users have to painstakingly re-filter the dates and manually concatenate the data.
    </li>
    <li>
        <h4>Manually re-filtering data is highly inefficient and risks innacuracies</h4>
    </li>
    <li>
        <h4>Takes time away from doing actual analysis work</h4>
    </li>
</ul>

## Solution
Therefore, to solve this, I wrote a Python code that not only can retrieve the storm events data but also pre-process data to emulate the storm events summary table.

<ol>
    <li>
        <h3>Storm Data Auto-Query</h3>
        File: <code>storm_data_retriever.py</code>
        <br>The code to query and download the CSV file of the storm events data. This code also has the feature to check the HTTP status of the database.
        <br>
        <br>Content:
            <ol>
                <li>
                    Class: <code>search_storm_data</code>
                    <br>Selected Method: <code>get_storm_data</code>
                    <br>This method first checks that the HTTP status is "OK" (200) then it queries the daily storm events data from a given state. 
                </li>
                <li>
                    Class: <code>get_periodical_storm_events_data</code>
                    <br>Methods:
                        <ul>
                            <li>
                                <code>get_annual_storm_data</code>
                                <br>This method queries the annual storm data from a given year and state then returns it in pandas dataframe format.
                            </li>
                            <li>
                                <code>save_annual_storm_data</code>
                                <br>This method queries the annual storm data from a given year and state then saves the data in CSV format.
                            </li>
                        </ul> 
                </li>
                <li>
                    Class: <code>get_all_states_data</code>
                    <br>Method: <code>get_annual_data</code>
                    <br>This method queries the annual storm events data from all states from a given year and save them in CSV files (separated by state and year).
                </li>
            </ol>
    </li>
    <li>
        <h3>Storm Data Pre-Processing</h3>
        File: <code>TBD</code>
        <br>The code to loads storm events data from a given state and period of time, then pre-process it to obtain the periodical summaries.
        <br>Summary features:
            <ol>
                <li>Number of County/Zone areas affected</li>
                <li>Number of Days with Event</li>
                <li>Number of Days with Event and Death</li>
                <li>Number of Days with Event and Death or Injury</li>
                <li>Number of Days with Event and Property Damage</li>
                <li>Number of Days with Event and Crop Damage</li>
                <li>Number of Event Types reported</li>
                <li>Direct Deaths from the Event</li>
                <li>Direct Injuries from the Event</li>
                <li>Property Damage Estimate</li>
                <li>Crop Damage Estimate</li>
            </ol>
    </li>
</ol>

## Sources
<ul>
<li>US States and Counties list: <a href = "https://gist.github.com/vitalii-z8i/">vitalii-z8i</a></li>
<li>US States FIPS code: I manually coded them from <a href = "https://www.ncdc.noaa.gov/stormevents/">NCDC NOAA's storm events database</a></li>
</ul>


