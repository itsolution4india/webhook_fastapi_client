import os
import re
import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio

import uvicorn
from fastapi import FastAPI, Request, Response, BackgroundTasks, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
import httpx

# Create FastAPI app
app = FastAPI(title="WhatsApp Webhook Service")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constants
VERIFY_TOKEN = "hello"
LOGS_DIR = 'message_logs'
DASHBOARD_URL = "https://main.wtsmessage.xyz/user_responses/"

# Set up logging
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'app.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)

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
connection_pool = MySQLConnectionPool(pool_name="mypool", pool_size=10, **dbconfig)

# Get database connection from pool
def get_connection():
    try:
        return connection_pool.get_connection()
    except Exception as e:
        logging.error(f"Failed to get database connection: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# Helper functions
def remove_special_chars(text: str) -> str:
    """Remove non-ASCII characters from text."""
    if not text:
        return ""
    return re.sub(r'[^\x00-\x7F]+', '', text)

def clean_interactive_type(interactive_type):
    cleaned_interactive_type = {}
    for key, value in interactive_type.items():
        if key == "flow_token":
            continue
        new_key = "_".join(key.split('_')[:-1]).replace('_', ' ')
        
        if isinstance(value, str) and '_' in value:
            value = value.split('_', 1)[1]
        
        cleaned_interactive_type[new_key] = value

    return cleaned_interactive_type

def parse_webhook_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Parse webhook response into standardized format."""
    report = {}
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    report['Date'] = formatted_datetime
    
    for entry in response.get('entry', []):
        changes = entry.get('changes', [])
        for change in changes:
            value = change.get('value', {})
            metadata = value.get('metadata', {})
            report['display_phone_number'] = metadata.get('display_phone_number')
            report['phone_number_id'] = metadata.get('phone_number_id')
            
            message_template_id = value.get('message_template_id')
            message_template_name = value.get('message_template_name')

            if message_template_id and message_template_name:
                report['message_template_id'] = message_template_id
                report['message_template_name'] = message_template_name
                
            statuses = value.get('statuses', [])
            for status in statuses:
                report['waba_id'] = status.get('id')
                report['status'] = status.get('status')
                report['message_timestamp'] = status.get('timestamp')
                report['contact_wa_id'] = status.get('recipient_id')
                if 'errors' in status:
                    error_details = status['errors'][0]
                    report['error_code'] = error_details.get('code')
                    report['error_title'] = error_details.get('title')
                    report['error_message'] = error_details.get('message')
                    report['error_data'] = error_details.get('error_data', {}).get('details')

            contacts = value.get('contacts', [])
            for contact in contacts:
                contact_name = contact.get('profile', {}).get('name', '')
                try:
                    report['contact_name'] = remove_special_chars(contact_name)
                except:
                    report['contact_name'] = ''
                report['contact_wa_id'] = contact.get('wa_id')

            messages = value.get('messages', [])
            for message in messages:
                report['message_from'] = message.get('from')
                report['status'] = 'reply'
                report['waba_id'] = message.get('id')
                report['message_timestamp'] = message.get('timestamp')
                report['message_type'] = message.get('type')
                
                if message.get('type') == 'text':
                    report['message_body'] = message.get('text', {}).get('body')
                elif message.get('type') == 'button':
                    report['message_body'] = message.get('button', {}).get('text')
                elif message.get('type') == 'image':
                    report['message_body'] = message.get('image', {}).get('id')
                elif message.get('type') == 'document':
                    report['message_body'] = message.get('document', {}).get('id')
                elif message.get('type') == 'video':
                    report['message_body'] = message.get('video', {}).get('id')
                elif message.get('type') == 'interactive':
                    interactive_msg_str = message.get('interactive', {}).get('type')
                    interactive_msg_json = json.loads(interactive_msg_str)
                    interactive_type = clean_interactive_type(interactive_msg_json)
                    if interactive_type == 'button_reply':
                        report['message_body'] = message.get('interactive', {}).get('button_reply', {}).get('title')
                    elif interactive_type == 'list_reply':
                        report['message_body'] = message.get('interactive', {}).get('list_reply', {}).get('title')
                    elif interactive_type == 'nfm_reply':
                        report['message_body'] = message.get('interactive', {}).get('nfm_reply', {}).get('response_json')
                else:
                    report['message_body'] = ""
    
    return report

async def send_to_dashboard(response: Dict[str, Any]):
    """Send webhook response to dashboard."""
    try:
        # Extract message text for logging
        message_text = None
        interactive_text = None
        user_response = None
        
        try:
            message_text = response['entry'][0]['changes'][0]['value']['messages'][0]['button']['text']
        except (KeyError, IndexError):
            pass
            
        try:
            interactive_text = response['entry'][0]['changes'][0]['value']['messages'][0]['interactive']['button_reply']['title']
        except (KeyError, IndexError):
            pass
        
        try:
            user_response = response['entry'][0]['changes'][0]['value']['messages'][0]['text']['body']
        except (KeyError, IndexError):
            pass
        
        if message_text or user_response or interactive_text:
            async with httpx.AsyncClient() as client:
                dashboard_response = await client.post(DASHBOARD_URL, json={'response': response})
                if dashboard_response.status_code == 200:
                    logging.info(f"Response successfully sent to the dashboard. {response}")
                else:
                    logging.warning(f"Failed to send response to dashboard. Status code: {dashboard_response.status_code}, Response: {dashboard_response.text}")
    except Exception as e:
        logging.error(f"Error sending response to dashboard: {e}")

async def create_account_table(account_id: str, connection):
    """Create a new table for an account if it doesn't exist."""
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        table_name = f"webhook_responses_{account_id}"
        check_table_query = (
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{dbconfig['database']}' AND table_name = '{table_name}'"
        )
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            # Create table with same structure as webhook_responses
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
                message_template_name VARCHAR(100)
            )
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

async def store_webhook_data(report: Dict[str, Any], body: Dict[str, Any], account_id: str):
    """Store webhook data in the database."""
    connection = None
    cursor = None
    
    try:
        connection = get_connection()
        
        # Determine which table to use
        target_table = await create_account_table(account_id, connection)
        cursor = connection.cursor()
            

        check_query = f"SELECT COUNT(*) FROM {target_table} WHERE waba_id = %s"
        cursor.execute(check_query, (report.get('waba_id'),))
        count = cursor.fetchone()[0]
        
        if count == 0:
            
            asyncio.create_task(send_to_dashboard(body))
            
            # Insert new record
            add_response = (
                f"INSERT INTO {target_table} "
                "(Date, display_phone_number, phone_number_id, waba_id, contact_wa_id, status, "
                "message_timestamp, error_code, error_message, contact_name, message_from, "
                "message_type, message_body) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )

            data_response = (
                report.get('Date'),
                report.get('display_phone_number'),
                report.get('phone_number_id'),
                report.get('waba_id'),
                report.get('contact_wa_id'),
                report.get('status'),
                report.get('message_timestamp'),
                report.get('error_code'),
                report.get('error_message'),
                report.get('contact_name'),
                report.get('message_from'),
                report.get('message_type'),
                report.get('message_body')
            )

            cursor.execute(add_response, data_response)
            connection.commit()
        else:
            # Update existing record
            update_query = (
                f"UPDATE {target_table} SET "
                "Date = %s, display_phone_number = %s, phone_number_id = %s, contact_wa_id = %s, "
                "status = %s, message_timestamp = %s, error_code = %s, error_message = %s, "
                "contact_name = %s, message_from = %s, message_type = %s, message_body = %s "
                "WHERE waba_id = %s"
            )

            update_data = (
                report.get('Date'),
                report.get('display_phone_number'),
                report.get('phone_number_id'),
                report.get('contact_wa_id'),
                report.get('status'),
                report.get('message_timestamp'),
                report.get('error_code'),
                report.get('error_message'),
                report.get('contact_name'),
                report.get('message_from'),
                report.get('message_type'),
                report.get('message_body'),
                report.get('waba_id')
            )

            cursor.execute(update_query, update_data)
            connection.commit()

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(err)}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Background task to handle webhook data
async def process_webhook(body: Dict[str, Any], account_id: str):
    """Process webhook data asynchronously."""
    try:
        # Parse webhook response
        data = parse_webhook_response(body)
        
        await store_webhook_data(data, body, account_id)
        
    except Exception as e:
        logging.exception(f"Error in background processing: {e}")

# Endpoints
@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "WhatsApp Webhook Service is running"}

@app.get("/{account_id}/")
async def verify_webhook(account_id: str, request: Request):
    """Verify webhook endpoint for specific account."""
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    logging.debug(f"Verification for account {account_id} - Mode: {mode}, Token: {token}, Challenge: {challenge}")

    if mode and token:
        if mode == 'subscribe' and token == VERIFY_TOKEN:
            return Response(content=challenge, media_type="text/plain")
        else:
            logging.warning(f"Webhook verification failed for account {account_id}: Invalid token.")
            return Response(content="Verification token mismatch", status_code=403)
    else:
        logging.warning(f"Webhook verification failed for account {account_id}: Missing parameters.")
        return Response(content="Bad request parameters", status_code=400)

@app.post("/{account_id}/")
async def handle_webhook(account_id: str, request: Request, background_tasks: BackgroundTasks):
    """Handle webhook for specific account."""
    try:
        body = await request.json()
        
        if not body:
            logging.warning(f"Received empty JSON payload for account {account_id}.")
            return {"status": "ok"}

        if 'entry' not in body or not body['entry']:
            logging.warning(f"Invalid webhook payload for account {account_id}: {body}")
            return {"status": "ok"}

        # Process webhook in background
        background_tasks.add_task(process_webhook, body, account_id)
        
        return {"status": "ok"}

    except Exception as e:
        logging.exception(f"Error processing message for account {account_id}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/status")
async def check_status():
    """Health check endpoint."""
    return {"status": "online", "version": "1.0"}

if __name__ == "__main__":
    logging.info("Starting FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000)