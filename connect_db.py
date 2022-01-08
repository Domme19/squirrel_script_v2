import pymysql as pms

def connect_to_db(): 
    DETAILS = ('root', '', 'localhost', 'testdb')
    conn = None
    try:
        conn = pms.connect(
            user=DETAILS[0], 
            password=DETAILS[1], 
            host=DETAILS[2], 
            database=DETAILS[3]
            )
        print("Connection to MySQL DB successful")
    except pms.Error as e:
        print(f"The error {e} occured.")
           
    return conn
    