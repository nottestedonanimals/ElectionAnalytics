import requests
# import urllib2
from lxml import etree
from bs4 import BeautifulSoup
import csv
import re
import database

def download(state_url, state):


    page = urllib2.urlopen(state_url)

    soup = BeautifulSoup(page)

    link = soup.find("a", attrs={"href": re.compile(r"https://voterinfo.sbe.virginia.gov/SBE_CSV/ELECTIONS/ELECTIONRESULTS/\d\d\d\d/\d\d\d\d%\d\dNovember%\d\dGeneral.csv")})["href"]

    with open(state + "_2016", "wb") as wf:

        wr = csv.writer(wf)
        wr.writerows(urllib2.urlopen(link))

def read_csv():

    vote_data = []

    with open("CurrentData\\VA_2012.csv", "r") as rf:

        rd = csv.reader(rf)
        headers = next(rd, list)
        candidate_index = [x for x in range(0, len(headers)) if x > 2]
        for row in rd:
            if row[0] == "" or row[0].lower() == "totals":
                continue

            precinct = row[2].split(" - ")
            county = row[0]

            for i in candidate_index:
                if len(precinct) == 2:

                    vote_data.append((headers[i], county, precinct[0], precinct[1], row[i], "VA"))

                else:

                    vote_data.append((headers[i], county, -999, precinct[0], row[i], "VA"))

    for v in vote_data:

        db = database.PresidentialDatabase("presidential_2012")
        db.insert_location_data((v[2], v[3], v[1], v[5]))

    return vote_data


if __name__ == "__main__":

    # download("http://elections.virginia.gov/media/election-night-reporting/index.html", "VA")
    db = database.PresidentialDatabase("presidential_2012")
    db.create_table()
    read_csv()
