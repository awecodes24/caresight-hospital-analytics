

from db.connection import get_connection

def load_data():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM patients LIMIT 5;")
    
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    cursor.close()
    conn.close()