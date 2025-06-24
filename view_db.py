import sqlite3
conn = sqlite3.connect('banking.db')
for row in conn.execute('SELECT * FROM transactions'):
    print(row)
conn.close()