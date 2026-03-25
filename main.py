
from db.connection import get_connection

def test_connection():
    try:
        conn = get_connection()
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user;")
            db, user = cursor.fetchone()
            
            print(f"✅ Connected to DB: {db} as user: {user}")
        
        conn.close()
        
    except Exception as e:
        print("❌ Connection failed:", e)

if __name__ == "__main__":
    test_connection()