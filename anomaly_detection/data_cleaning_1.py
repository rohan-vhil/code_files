import pandas as pd
import numpy as np
import json


class SolarDataCleaner:

    def __init__(self, inverter_capacity_kw=350):
        self.inverter_capacity = inverter_capacity_kw

        # Modbus error values
        self.error_values = [65535, 65534, 65533, -1]

        # Physical limits
        self.limits = {
            "voltage": (0, 1000),
            "current": (0, 400),
            "temperature": (-20, 90),
            "frequency": (45, 65),
            "power": (0, inverter_capacity_kw * 1000)
        }

    # --------------------------------------------------
    # STEP 1: LOAD DATA
    # --------------------------------------------------

    def load_csv(self, path):
        df = pd.read_csv(path)
        print("Loaded rows:", len(df))
        return df

    # --------------------------------------------------
    # STEP 2: EXTRACT JSON
    # --------------------------------------------------

    def extract_json(self, df):

        df["data"] = df["data"].apply(json.loads)

        json_cols = pd.json_normalize(df["data"])

        df = pd.concat([df.drop(columns=["data"]), json_cols], axis=1)

        print("JSON fields extracted")

        return df

    # --------------------------------------------------
    # STEP 3: REMOVE DUPLICATES
    # --------------------------------------------------

    def remove_duplicates(self, df):

        before = len(df)

        df = df.drop_duplicates(subset=["timestamp", "device_id"])

        after = len(df)

        print("Duplicates removed:", before - after)

        return df

    # --------------------------------------------------
    # STEP 4: CONVERT DATATYPES
    # --------------------------------------------------

    def convert_types(self, df):

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        numeric_cols = df.columns.difference(["timestamp", "device_id"])

        df[numeric_cols] = df[numeric_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        print("Datatype conversion completed")

        return df

    # --------------------------------------------------
    # STEP 5: REMOVE MODBUS ERROR VALUES
    # --------------------------------------------------

    def remove_error_values(self, df):

        df.replace(self.error_values, np.nan, inplace=True)

        print("Modbus error values cleaned")

        return df

    # --------------------------------------------------
    # STEP 6: REMOVE IMPOSSIBLE PHYSICAL VALUES
    # --------------------------------------------------

    def remove_out_of_range(self, df):

        for col in df.columns:

            if "voltage" in col.lower():

                df.loc[
                    (df[col] < self.limits["voltage"][0]) |
                    (df[col] > self.limits["voltage"][1]),
                    col
                ] = np.nan

            elif "current" in col.lower():

                df.loc[
                    (df[col] < self.limits["current"][0]) |
                    (df[col] > self.limits["current"][1]),
                    col
                ] = np.nan

            elif "temp" in col.lower():

                df.loc[
                    (df[col] < self.limits["temperature"][0]) |
                    (df[col] > self.limits["temperature"][1]),
                    col
                ] = np.nan

            elif "freq" in col.lower():

                df.loc[
                    (df[col] < self.limits["frequency"][0]) |
                    (df[col] > self.limits["frequency"][1]),
                    col
                ] = np.nan

            elif "power" in col.lower():

                df.loc[
                    (df[col] < self.limits["power"][0]) |
                    (df[col] > self.limits["power"][1]),
                    col
                ] = np.nan

        print("Physical range validation completed")

        return df

    # --------------------------------------------------
    # STEP 7: DETECT TIMESTAMP GAPS
    # --------------------------------------------------

    def detect_gaps(self, df):

        df = df.sort_values(["device_id", "timestamp"])

        df["time_gap_sec"] = (
            df.groupby("device_id")["timestamp"]
            .diff()
            .dt.total_seconds()
        )

        print("Timestamp gap detection completed")

        return df

    # --------------------------------------------------
    # STEP 8: DETECT STUCK SENSORS
    # --------------------------------------------------

    def detect_stuck_sensors(self, df):

        sensor_cols = df.columns.difference(
            ["timestamp", "device_id", "time_gap_sec"]
        )

        for col in sensor_cols:

            diff = df.groupby("device_id")[col].diff()

            stuck = diff == 0

            df.loc[stuck, col] = np.nan

        print("Stuck sensor detection completed")

        return df

    # --------------------------------------------------
    # STEP 9: HANDLE MISSING VALUES
    # --------------------------------------------------

    def fill_missing(self, df):

        df = df.sort_values(["device_id", "timestamp"])

        df = df.groupby("device_id").apply(
            lambda g: g.interpolate(method="linear")
        )

        df = df.groupby("device_id").apply(
            lambda g: g.fillna(method="ffill")
        )

        df = df.reset_index(drop=True)

        print("Missing values handled")

        return df

    # --------------------------------------------------
    # STEP 10: FINAL SORT
    # --------------------------------------------------

    def final_sort(self, df):

        df = df.sort_values(["device_id", "timestamp"])

        return df

    # --------------------------------------------------
    # COMPLETE PIPELINE
    # --------------------------------------------------

    def run_pipeline(self, path):

        df = self.load_csv(path)

        df = self.extract_json(df)

        df = self.remove_duplicates(df)

        df = self.convert_types(df)

        df = self.remove_error_values(df)

        df = self.remove_out_of_range(df)

        df = self.detect_gaps(df)

        df = self.detect_stuck_sensors(df)

        df = self.fill_missing(df)

        df = self.final_sort(df)

        print("Data cleaning pipeline completed")

        return df


# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    cleaner = SolarDataCleaner(inverter_capacity_kw=350)

    clean_df = cleaner.run_pipeline("solar_data_export.csv")

    clean_df.to_csv("clean_solar_data.csv", index=False)

    print("Clean dataset saved")