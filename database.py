import sqlite3 as sql


class PresidentialDatabase():

    def __init__(self, database):

        self.cnxn = sql.connect(database)
        self.cn = self.cnxn.cursor()

        self.cn.execute("PRAGMA journal_mode='WAL'")
        self.cnxn.commit()

    def create_table(self):

        self.cn.execute("CREATE TABLE Candidates (Id INTEGER, Name TEXT)")
        self.cnxn.commit()

        self.cn.execute("CREATE TABLE Locations (Id INTEGER, PrecinctId INTEGER, Precinct TEXT, County TEXT, State TEXT)")
        self.cnxn.commit()

        self.cn.execute("CREATE TABLE Votes (CandidateId INTEGER, LocationId INTEGER, VoteCount INTEGER, TotalVotes INTEGER)")
        self.cnxn.commit()


    def create_candidate_table(self, candidates):

        candidate_mapping = []

        for c in candidates:

            candidate_mapping.append((candidates.index(c), c))

        self.cn.executemany("INSERT INTO Candidates (Id, Name) VALUES (?, ?)", candidate_mapping)
        self.cnxn.commit()

        self.cn.execute("SELECT * FROM Candidates")

    def insert_location_data(self, location_info):

        self.cn.execute("SELECT MAX(Id) FROM Locations")

        new_id = self.cn.fetchall()[0][0]

        if new_id is None:

            new_id = 0

        else:

            new_id += 1

        self.cn.execute("SELECT Id FROM Locations "
                        "WHERE Precinct=? and County=? and State=?", (location_info[1], location_info[2], location_info[3]))

        results = self.cn.fetchall()

        if not results:

            self.cn.execute("INSERT INTO Locations (Id, PrecinctId, Precinct, County, State) VALUES (?, ?, ?, ?, ?)",
                       (new_id, location_info[0], location_info[1], location_info[2], location_info[3]))

        self.cnxn.commit()

        self.cn.execute("SELECT * from Locations")
        self.cn.fetchall()

        new_id += 1

    def create_index(self):

        self.cn.execute("CREATE INDEX CandidateId ON Candidates (Id)")
        self.cnxn.commit()
        self.cn.execute("CREATE INDEX CandidatesName ON Candidates (Name)")
        self.cnxn.commit()

        self.cn.execute("CREATE INDEX LocationsId ON Locations (Od)")
        self.cnxn.commit()

    def drop_tables(self):

        self.cn.execute("DROP TABLE Candidates")
        self.cn.execute("DROP TABLE Locations")
        self.cn.execute("DROP TABLE votes")

        self.cnxn.commit()

if __name__ == "__main__":

    # create_table()
    # create_candidate_table(['Obama', 'Romney', 'Johnson', 'Goode', 'Stein'])

    db = PresidentialDatabase("presidential_2012.db")
    db.drop_tables()