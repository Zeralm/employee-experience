import imp
import mysql.connector
import pandas as pd
import datetime



def insert(values ,host="localhost", user="testuser", password="testuser", database="fifth_try"):
    """
    """
    con = mysql.connector.connect(
    host=host,
    user=user,
    password=password
    )
    cursObj = con.cursor()
    
    try:
        cursObj.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
        cursObj.execute(f"USE {database};")
        cursObj.execute(f"CREATE TABLE IF NOT EXISTS opinions(id INTEGER AUTO_INCREMENT, rating FLOAT, info TEXT, title TEXT, employment TEXT, pros TEXT, cons TEXT, time TEXT, company TEXT, glass_id TEXT, PRIMARY KEY(id));")
        cursObj.executemany("INSERT INTO opinions(rating, info, title, employment, pros, cons, time, company, glass_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);", list(values.itertuples(index=False, name=None)))
        con.commit()
    except Exception as exc:
        print(f"Attempt to insert rows failed: {exc}")

    finally:
        cursObj.close()
        con.close()
        print(f"{len(values)} values loaded")



# [3.0, "info", "title", "employment", "pros", "cons", "time", "company", "glass_id"]

# insert(io)
def extract(host="localhost", user="testuser", password="testuser", database="first_try"):
    """
    """
    con = mysql.connector.connect(
    host=host,
    user=user,
    password=password
    )
    cursObj = con.cursor()
    try:        
        cursObj.execute(f"USE {database}")
        cursObj.execute("SELECT * FROM opinions;")
        out = cursObj.fetchall()
    finally:
        cursObj.close()
        con.close()
    print(f"{len(out)} values extracted")
    return out

def send(host="34.79.95.247", user="parse-test", password="parse-test", database="glass_data"):
    locs = pd.DataFrame(extract())
    locs["staging"] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    values = locs.copy()
    con = mysql.connector.connect(
    host=host,
    user=user,
    password=password
    )
    cursObj = con.cursor()
    try:
        cursObj.execute(f"USE {database}")
        cursObj.executemany("INSERT INTO opinions(rating, info, title, employment, pros, cons, time, company, glass_id, add_staging) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", list(values.itertuples(index=False, name=None)))
        con.commit()
    finally:
        cursObj.close()
        con.close()
        print(f"{len(values)} values loaded remotely")
    
    del locs
    del values

# print(pd.DataFrame(extract()).iloc[45])
# cursObj.execute("SELECT * FROM opinions;")
# print(cursObj.fetchall())




# CREATE ONE TYPE OF PARTICULAR FUNCTION FOR GATHERING AND A GENERAL-END TYPE OF FUNCTION 
# IN WHICH YOU SPECIFY THE QUERY AND THE PARAMETERS

# cursObj.execute("CREATE TABLE opinions(id INTEGER AUTO_INCREMENT, rating FLOAT, info TEXT, title TEXT, employment TEXT, pros TEXT, cons TEXT, time TEXT, company TEXT, glass_id TEXT, PRIMARY KEY(id))")
# con.commit()

# Check for unique later