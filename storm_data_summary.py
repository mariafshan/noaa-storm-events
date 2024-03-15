import pandas as pd

class summarize_storm_data:
    """
    This class summarize NOAA storms events data.

    Methods
    -------
    noaa_standard_summary():
        Summarize storm events data based on the NOAA website summary.
    """
    def noaa_standard_summary(self, data: pd.DataFrame):
        """
        Summarize storm events data based on the NOAA website summary.
        Reference: https://www.ncdc.noaa.gov/stormevents/

        Meta Data
            affected_cz: Number of County/Zone areas affected
            days_event: Number of Days with Event
            days_event_death: Number of Days with Event and Death
            days_event_death_injury: Number of Days with Event and Death or Injury
            days_property_dmg: Number of Days with Event and Property Damage
            days_crop_dmg: Number of Days with Event and Crop Damage
            num_event_types: Number of Event Types reported
            deaths: Direct Deaths from the Event
            injuries: Direct Injuries from the Event
            property_damage: Property Damage Estimate
            crop_damage: Crop Damage Estimate

        Parameters
        ----------
        data: pandas Dataframe

        Returns
        -------
        summary_data: pandas Dataframe

        """
        days_of_death = 0
        days_of_death_injury = 0
        days_of_property_dmg = 0
        days_of_crop_dmg = 0

        for date in data["BEGIN_DATE"].unique():
            if data[data["BEGIN_DATE"] == date]["DEATHS_DIRECT"].sum() > 0:
                days_of_death += 1

            if (data[data["BEGIN_DATE"] == date]["DEATHS_DIRECT"].sum() > 0) or (data[data["BEGIN_DATE"] == date]["INJURIES_DIRECT"].sum() > 0):
                days_of_death_injury += 1

            if (data[data["BEGIN_DATE"] == date]["DAMAGE_PROPERTY_NUM"].sum() > 0):
                days_of_property_dmg += 1

            if (data[data["BEGIN_DATE"] == date]["DAMAGE_CROPS_NUM"].sum() > 0):
                days_of_crop_dmg += 1

        summary_df = pd.DataFrame()
        summary_df["affected_cz"] = [len(data["CZ_NAME_STR"].unique())]
        summary_df["days_event"] = [len(data["BEGIN_DATE"].unique())]
        summary_df["days_event_death"] = [days_of_death]
        summary_df["days_event_death_injury"] = [days_of_death_injury]
        summary_df["days_property_dmg"] = [days_of_property_dmg]
        summary_df["days_crop_dmg"] = [days_of_crop_dmg]
        summary_df["num_event_types"] = [len(data["EVENT_TYPE"].unique())]
        summary_df["deaths"] = [data["DEATHS_DIRECT"].sum()]
        summary_df["injuries"] = [data["INJURIES_DIRECT"].sum()]
        summary_df["property_damage"] = [data["DAMAGE_PROPERTY_NUM"].sum() / 1000]
        summary_df["crop_damage"] = [data["DAMAGE_CROPS_NUM"].sum() / 1000]

        return summary_df