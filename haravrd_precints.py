import csv
import pandas
import os


class PrecintData:

    def __init__(self, directory):

        self.directory = directory
        self.f_list = os.listdir(self.directory)
        self.data = []

    def read_data(self):

        data = []

        for f in self.f_list:

            df = pandas.read_csv(os.path.join(self.directory, f), header=0, sep="\t", encoding="utf-8")

            if f[:2] == "AK":

                self.read_alaska(df)

    def read_alaska(self, df):

        if "ed_precint" not in df.columns and "precinct_code" not in df.columns:

            df["precinct_code"] = df["precinct"].str.extract(r"(\d+\-\d+)")
            df2 = pandas.DataFrame(df.precinct.str.extract(r"(?<=District\s)(\d\d*)"), columns=["Index", "precinct"])
            df2.rename(columns={"precinct": "district"}, inplace=True)
            df2.drop("Index", axis=1, inplace=True)
            df2["district"] = df2.district.str.zfill(2)+"-000"
            df = df.join(df2, how="left")

            df.loc[df.precinct_code.isnull(), "precinct_code"] = df.district
            df.drop("district", axis=1, inplace=True)
            print(df.head(10))




if __name__ == "__main__":

    pd = PrecintData("CurrentData\\dataverse_files")
    pd.read_data()