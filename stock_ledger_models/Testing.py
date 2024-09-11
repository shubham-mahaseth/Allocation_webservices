from connect import curr


conn=curr()
cur = conn.cursor()


# Use all the SQL you like
mycon=cur.execute("SELECT CONNECTION_ID();")
mycon=cur.fetchall()
print("khgsiu::",mycon[0],conn)

cur.execute("SELECT * FROM warehouse limit 1")

# print all the first cell of all the rows
for row in cur.fetchall():
    print(row)