import sqlite3 as sql
import csv

sqlConnection = sql.connect('database.db')

c = sqlConnection.cursor()

c.execute("""
    CREATE TABLE Locus(
        ID text PRIMARY KEY,
        Start integer,
        End integer
    );""")

c.execute("""
    CREATE TABLE Gene(
        Start integer,
        End integer,
        DN_expression real,
        PGN_expression real,
        Length integer,
        Gene text PRIMARY KEY,
        Chromosome text
    );""")

c.execute("""
    CREATE TABLE DN_Interactions(
        Locus1 text,
        Locus2 text,
        InteractionID text PRIMARY KEY,
        FOREIGN KEY (Locus1) REFERENCES Locus (ID),
        FOREIGN KEY (Locus2) REFERENCES Locus (ID)
    );""")

c.execute("""
    CREATE TABLE PGN_Interactions(
        Locus1 text,
        Locus2 text,
        InteractionID text PRIMARY KEY,
        FOREIGN KEY (Locus1) REFERENCES Locus (ID),
        FOREIGN KEY (Locus2) REFERENCES Locus (ID)
    );""")

c.execute("""
    CREATE TABLE Motif_Info(
        Model text PRIMARY KEY,
        Transcription_Factor text,
        Model_Length integer,
        Quality text,
        TF_Family text,
        EntrezGene integer,
        UniProt_ID text
    );""")

c.execute("""
    CREATE TABLE DN_Motif_Instance(
        Chromosome text,
        Locus_Start integer,
        Locus_End integer,
        Locus_ID text,
        Motif_Start integer,
        Motif_End integer,
        Model text,
        P_Val real,
        Direction text,
        FOREIGN KEY (Model) REFERENCES Motif_Info (Model),
        FOREIGN KEY (Locus_ID) REFERENCES Locus (ID)
    );""")

c.execute("""
    CREATE TABLE PGN_Motif_Instance(
        Chromosome text,
        Locus_Start integer,
        Locus_End integer,
        Locus_ID text,
        Motif_Start integer,
        Motif_End integer,
        Model text,
        P_Val real,
        Direction text,
        FOREIGN KEY (Model) REFERENCES Motif_Info (Model),
        FOREIGN KEY (Locus_ID) REFERENCES Locus (ID)
    );""")

c.execute("""
    CREATE TABLE DN_Counts(
        Locus_ID text,
        Count integer
    );""")

c.execute("""
    CREATE TABLE PGN_Counts(
        Locus_ID text,
        Count integer
    );""")

with open("Expression_PGN_DN_filter_repeat_gene.csv") as Genes:
    for row in csv.DictReader(Genes):
        #c.execute("""INSERT INTO Chromosome VALUES (:chrom)""", row)
        c.execute("""INSERT INTO Gene VALUES (:start, :stop, :DN_expression, :PGN_expression, :length, :gene, :chrom)""", row)

with open("DN_Interactions.txt") as DN_Interactions:
    for row in csv.DictReader(DN_Interactions, delimiter='\t'):
        #print(row["Locus1"], row["Locus2"])
        InteractionID = row["Locus1"] + row["Locus2"]
        row["InteractionID"] = InteractionID
        c.execute("""INSERT INTO DN_Interactions VALUES (:Locus1, :Locus2, :InteractionID)""", row)

with open("PGN_Interactions.txt") as PGN_Interactions:
    for row in csv.DictReader(PGN_Interactions, delimiter='\t'):
        InteractionID = row["Locus1"] + row["Locus2"]
        row["InteractionID"] = InteractionID
        c.execute("""INSERT INTO PGN_Interactions VALUES (:Locus1, :Locus2, :InteractionID)""", row)

with open("50_TF.tsv") as Motif_Info:
    readerCSV = csv.reader(Motif_Info, delimiter='\t')
    # Grabs first line and removes it (containing headers)
    headers=next(readerCSV)
    for row in readerCSV:
        c.execute("""INSERT INTO Motif_Info VALUES (?, ?, ?, ?, ?, ?, ?)""", (row[0], row[2], row[3], row[4], row[5], row[6], row[7]))


with open("DN_motif_window_intersect.bed") as DN_motif_window_intersect:
    for row in csv.reader(DN_motif_window_intersect, delimiter='\t'):
        c.execute("""INSERT INTO DN_Motif_Instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (row[0], row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9]))

with open("PGN_motif_window_intersect.bed") as PGN_motif_window_intersect:
    for row in csv.reader(PGN_motif_window_intersect, delimiter='\t'):
        c.execute("""INSERT INTO PGN_Motif_Instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (row[0], row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9]))

c.execute("""INSERT INTO Locus SELECT DISTINCT Locus_ID, Locus_Start, Locus_End from (Select Locus_ID, Locus_Start, Locus_End from DN_Motif_Instance union Select Locus_ID, Locus_Start, Locus_End from PGN_Motif_Instance)""")

c.execute("""DELETE FROM DN_Motif_Instance""")
c.execute("""DELETE FROM PGN_Motif_Instance""")

with open("DN_motif_window_intersect.bed") as DN_motif_window_intersect:
    for row in csv.reader(DN_motif_window_intersect, delimiter='\t'):
        c.execute("""INSERT INTO DN_Motif_Instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (row[0], row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9]))

with open("PGN_motif_window_intersect.bed") as PGN_motif_window_intersect:
    for row in csv.reader(PGN_motif_window_intersect, delimiter='\t'):
        c.execute("""INSERT INTO PGN_Motif_Instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (row[0], row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9]))

c.execute("""INSERT INTO DN_Counts SELECT DN_Interactions.Locus1, COUNT(DN_Motif_Instance.Locus_ID)
FROM DN_Interactions
LEFT JOIN DN_Motif_Instance
    ON DN_Interactions.Locus1 = DN_Motif_Instance.Locus_ID
GROUP BY DN_Interactions.Locus1 ;""")

c.execute("""INSERT INTO PGN_Counts SELECT PGN_Interactions.Locus1, COUNT(PGN_Motif_Instance.Locus_ID)
FROM PGN_Interactions
LEFT JOIN PGN_Motif_Instance
    ON PGN_Interactions.Locus1 = PGN_Motif_Instance.Locus_ID
GROUP BY PGN_Interactions.Locus1 ;""")

sqlConnection.commit()

sqlConnection.close()
