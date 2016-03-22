import sys
import pandas
import os
import sqlite3
import re

class PrecintData:

    def __init__(self, directory):

        self.directory = directory
        self.f_list = os.listdir(self.directory)
        self.data = []

        self.cnxn = sqlite3.connect("ElectionResults")
        self.cn = self.cnxn.cursor()

    def create_election_table(self):

        self.cn.execute("CREATE TABLE ElectionData (State TEXT, Year INTEGER, PresidentialTotal INTEGER, "
                        "")

    def read_data(self):

        all_data = pandas.DataFrame()

        for f in self.f_list:

            df = pandas.read_csv(os.path.join(self.directory, f), header=0, sep="\t", encoding="utf-8")

            if f[:2] == "AK":

                print(self.read_alaska(df).columns)

        print(all_data)

    def read_alaska(self, df):

        if "ed_precinct" not in df.columns and "precinct_code" not in df.columns:

            df["precinct_code"] = df["precinct"].str.extract(r"(\d+\-\d+)")
            df2 = pandas.DataFrame(df.precinct.str.extract(r"(?<=District\s)(\d\d*)"), columns=["Index", "precinct"])
            df2.rename(columns={"precinct": "district"}, inplace=True)
            df2.drop("Index", axis=1, inplace=True)
            df2["district"] = df2.district.str.zfill(2)+"-000"
            df = df.join(df2, how="left")

            df.loc[df.precinct_code.isnull(), "precinct_code"] = df.district
            df.drop("district", axis=1, inplace=True)

        else:

            df["precinct_code"] = df["precinct"]
            try:
                df.drop(["precinct"], axis=1, inplace=True)
            except ValueError:
                pass

            try:
                df.drop(["ed"], axis=1, inplace=True)
            except ValueError:
                pass

            try:
                df.drop(["ed_precinct"], inplace=True)
            except ValueError:
                pass

        final_df = df[df.columns[df.columns.to_series().str.contains(r"state|year|_USP_|_USH_|_STS_|_STH_|_GOV_|_USS_|_USH_|precinct|precinct_code")]]

        final_df.columns = final_df.columns.str.replace("\d+", "")

        return final_df




if __name__ == "__main__":

    pd = PrecintData("CurrentData\\dataverse_files")
    pd.read_data()