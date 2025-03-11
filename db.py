from mysql.connector.pooling import MySQLConnectionPool
from utils import logging
from fastapi import HTTPException

# Database configuration
dbconfig = {
    "host": "localhost",
    "port": 3306,
    "user": "prashanth@itsolution4india.com",
    "password": "Solution@97",
    "database": "webhook_responses",
    "auth_plugin": 'mysql_native_password'
}

# Create connection pool
connection_pool = MySQLConnectionPool(
    pool_name="mypool", 
    pool_size=30,
    pool_reset_session=True,
    host=dbconfig["host"],
    port=dbconfig["port"],
    user=dbconfig["user"],
    password=dbconfig["password"],
    database=dbconfig["database"],
    auth_plugin=dbconfig["auth_plugin"],
    use_pure=False,
    connection_timeout=5,
    autocommit=True
)

# Get database connection from pool
def get_connection():
    try:
        return connection_pool.get_connection()
    except Exception as e:
        logging.error(f"Failed to get database connection: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")
    
    
async def create_account_table(account_id: str, connection):
    """Create a new table for an account if it doesn't exist with proper indexing."""
    try:
        cursor = connection.cursor()
        
        # Check if table exists - use prepared statement
        table_name = f"webhook_responses_{account_id}"
        check_table_query = (
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s"
        )
        cursor.execute(check_table_query, (dbconfig['database'], table_name))
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            # Create table with optimized structure and indexes
            create_table_query = f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Date DATETIME,
                display_phone_number VARCHAR(20),
                phone_number_id VARCHAR(50),
                waba_id VARCHAR(100),
                contact_wa_id VARCHAR(50),
                status VARCHAR(20),
                message_timestamp VARCHAR(50),
                error_code VARCHAR(50),
                error_message TEXT,
                error_title VARCHAR(100),
                error_data TEXT,
                contact_name VARCHAR(255),
                message_from VARCHAR(50),
                message_type VARCHAR(20),
                message_body TEXT,
                message_template_id VARCHAR(100),
                message_template_name VARCHAR(100),
                INDEX idx_waba_id (waba_id),
                INDEX idx_contact_wa_id (contact_wa_id),
                INDEX idx_status (status),
                INDEX idx_date (Date)
            ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;
            """
            cursor.execute(create_table_query)
            connection.commit()
            logging.info(f"Created new table for account: {account_id}")
            
        return table_name
    except Exception as e:
        logging.error(f"Error creating account table: {e}")
        raise
    finally:
        if cursor:
            cursor.close()