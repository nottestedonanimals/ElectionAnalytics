import sys
import pandas
import os
import sqlite3
import re
import sys

class PrecintData:

    def __init__(self, directory):

        self.directory = directory
        self.f_list = os.listdir(self.directory)
        self.data = []

        self.cnxn = sqlite3.connect("ElectionResults")
        self.cn = self.cnxn.cursor()

    def create_election_table(self):

        self.cn.execute("""CREATE TABLE ElectionData (State TEXT, Year INTEGER, PresidentialTotal INTEGER)
                        """)

    def read_data(self):

        all_data = pandas.DataFrame(columns=["state", "year", "g_USP_tv", "g_USP_rv", "g_USP_dv", "g_USH_tv", "g_USH_rv", "g_USH_dv", "g_STS_tv", "g_STS_dv", "g_STS_dv", "g_STH_tv", "g_STH_rv", "g_STH_dv", "g_GOV_tv", "g_GOV_rv", "g_GOV_dv", "g_USS_tv", "g_USS_rv", "g_USS_dv", "precinct", "precinct_code"])

        for f in self.f_list:

            df = pandas.read_csv(os.path.join(self.directory, f), header=0, sep="\t", encoding="utf-8")

            if f[:2] == "AK":

                if len(all_data.index) == 0:

                    all_data = self.read_alaska(df)

                else:

                    all_data = pandas.concat([all_data, self.read_alaska(df)])


                # print(pandas.concat([all_data, self.read_alaska(df)]))

            else:

                if len(all_data.index) == 0:

                    all_data = self.read_basic(df)

                else:

                    all_data = pandas.concat([all_data, self.read_basic(df)])
            print(len(all_data.index))

        return all_data

    def read_alaska(self, df):


        try:

            df["precinct_code"] = df["ed_precinct"]
            df.drop("ed_precinct", axis=1, inplace=True)

        except KeyError:

            df["precinct_code"] = df["precinct"].str.extract(r"(\d+\-\d+)")
            df2 = pandas.DataFrame(df.precinct.str.extract(r"(?<=District\s)(\d\d*)"), columns=["Index", "precinct"])
            df2.rename(columns={"precinct": "district"}, inplace=True)
            df2.drop("Index", axis=1, inplace=True)
            df2["district"] = df2.district.str.zfill(2) + "-000"
            df = df.join(df2, how="left")

            df.loc[df.precinct_code.isnull(), "precinct_code"] = df.district
            df.drop("district", axis=1, inplace=True)

        try:
            df.drop(["ed"], axis=1, inplace=True)
        except ValueError:
            pass

        try:
            df.drop(["ed_precinct"], inplace=True)
        except ValueError:
            pass

        final_df = df[df.columns[df.columns.to_series().str.contains(r"state|year|_USP_|_USH_|_STS_|_STH_|_GOV_|_USS_|_USH_|precinct|precinct_code")]]

        final_df.columns = final_df.columns.to_series().str.replace("\d+", "")

        return final_df

    def read_basic(self, df):

        df2 = df[df.columns[df.columns.to_series().str.contains(r"state|year|_USP_|_USH_|_STS_|_STH_|_GOV_|_USS_|_USH_|precinct|precinct_code")]]

        df2.columns = df2.columns.to_series().str.replace("\d+(?=_)", "")

        return df2

    def read_alabama(self, df):

        df2 = df[df.columns[df.columns.to_series().str.contains(r"state|year|_USP_|_USH_|_STS_|_STH_|_GOV_|_USS_|_USH_|precinct|precinct_code")]]

        df2.columns = df2.columns.to_series().str.replace("\d+_", "")





if __name__ == "__main__":

    pd = PrecintData("CurrentData\\dataverse_files")
    print(pd.read_data())