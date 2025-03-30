import pymongo
import psycopg2
import os

def lambda_handler(event, context):
    # Setup MongoDB connection parameters (can be overridden by event parameters)
    mongo_host = event.get("mongo_host", os.getenv("MONGO_HOST", "localhost"))
    mongo_port = event.get("mongo_port", os.getenv("MONGO_PORT", 27017))
    mongo_db = event.get("mongo_db", os.getenv("MONGO_DB", "fruitDB"))
    mongo_collection = event.get("mongo_collection", os.getenv("MONGO_COLLECTION", "fruits"))
    
    # Setup PostgreSQL connection parameters (can be overridden by event parameters)
    pg_host = event.get("pg_host", os.getenv("PG_HOST", "localhost"))
    pg_port = event.get("pg_port", os.getenv("PG_PORT", 5432))
    pg_db = event.get("pg_db", os.getenv("PG_DB", "fruitdb"))
    pg_user = event.get("pg_user", os.getenv("PG_USER", "postgres"))
    pg_password = event.get("pg_password", os.getenv("PG_PASSWORD", "password"))
    
    # Connect to MongoDB
    try:
        mongo_client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
        mongo_db_conn = mongo_client[mongo_db]
        collection = mongo_db_conn[mongo_collection]
        print(f"Connected to MongoDB at {mongo_host}:{mongo_port}")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return {"status": "failed", "error": str(e)}
    
    # Extract fruit types from MongoDB
    try:
        fruit_data = collection.find({}, {"_id": 0, "name": 1})  # Only fetch 'name' field, excluding the '_id'
        fruit_types = [fruit["name"] for fruit in fruit_data]
        print(f"Extracted {len(fruit_types)} fruit types from MongoDB")
    except Exception as e:
        print(f"Error reading data from MongoDB: {e}")
        return {"status": "failed", "error": str(e)}
    
    # Connect to PostgreSQL
    try:
        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_password
        )
        pg_cursor = pg_conn.cursor()
        print(f"Connected to PostgreSQL at {pg_host}:{pg_port}")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return {"status": "failed", "error": str(e)}
    
    # Insert fruit types into PostgreSQL
    try:
        for fruit in fruit_types:
            pg_cursor.execute("INSERT INTO fruit_table (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (fruit,))
        pg_conn.commit()
        print(f"Inserted {len(fruit_types)} fruit types into PostgreSQL")
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")
        pg_conn.rollback()
        return {"status": "failed", "error": str(e)}
    
    # Clean up and close connections
    finally:
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()
    
    return {"status": "success", "message": "ETL process completed successfully"}
