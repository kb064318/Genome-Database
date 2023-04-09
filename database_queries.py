import sqlite3 as sql
import csv

sqlConnection = sql.connect('database.db')

c = sqlConnection.cursor()

# Question 1
print("Interactions specific to a cell type")
c.execute("Select * FROM DN_Interactions EXCEPT Select * from PGN_Interactions")
c.execute("Select * FROM PGN_Interactions EXCEPT Select * from DN_Interactions")
print(c.fetchall())
print()

c.execute("""SELECT * FROM DN_Motif_Instance CROSS JOIN PGN_Motif_Instance ON DN_Motif_Instance.Locus_ID = PGN_Motif_Instance.Locus_ID CROSS JOIN DN_Interactions ON DN_Interactions.Locus1 = DN_Motif_Instance.Locus_ID OR DN_Interactions.Locus2 = DN_Motif_Instance.Locus_ID CROSS JOIN PGN_Interactions ON PGN_Interactions.Locus1 = PGN_Motif_Instance.Locus_ID OR PGN_Interactions.Locus2 = PGN_Motif_Instance.Locus_ID LIMIT 10;
""")
print(c.fetchall())
print()

# Question 2
print("Interesting interactions between genes by count")
c.execute("""SELECT * FROM DN_Interactions, DN_Counts WHERE DN_Counts.Locus_ID = DN_Interactions.Locus1 ORDER BY DN_Counts.Count""")
print(c.fetchall())
print()

# Question 3
print("Different Gene Expressions")
c.execute("Select * FROM Gene WHERE DN_expression > 0 AND PGN_expression = 0")
print(c.fetchall())
print()
c.execute("Select Gene FROM Gene WHERE DN_expression = 0 AND PGN_expression > 0")
print(c.fetchall())

print()

# Question 4
print("Genes PGN - DN")
c.execute("Select * FROM Gene ORDER BY PGN_expression - DN_expression LIMIT 10")
print(c.fetchall())
print()
print("Genes DN - PGN")
c.execute("Select * FROM Gene ORDER BY DN_expression - PGN_expression LIMIT 10")
print(c.fetchall())

print()

sqlConnection.commit()

sqlConnection.close()
