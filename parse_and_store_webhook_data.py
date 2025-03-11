from typing import Dict, Any
from datetime import datetime
import asyncio
import json
from db import get_connection, create_account_table
import mysql.connector
from fastapi import HTTPException
from utils import remove_special_chars, logging, clean_interactive_type, DASHBOARD_URL

import time
import httpx
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


class WebhookBatcher:
    """
    Batches webhook requests in memory and processes them in larger groups
    to reduce database load.
    """
    def __init__(self, batch_size=50, max_wait_time=1.0):
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.batches = defaultdict(list)
        self.last_processed = defaultdict(float)
        self.locks = defaultdict(asyncio.Lock)
        self.processing = defaultdict(bool)
    
    async def add_request(self, report: Dict[str, Any], body: Dict[str, Any], account_id: str):
        """
        Add a request to the batch. Process immediately if batch size is reached
        or it's been too long since the last processing.
        """
        async with self.locks[account_id]:
            self.batches[account_id].append((report, body))
            current_time = time.time()
            
            if (len(self.batches[account_id]) >= self.batch_size or 
                (current_time - self.last_processed[account_id] >= self.max_wait_time and 
                 len(self.batches[account_id]) > 0)):
                
                if not self.processing[account_id]:
                    self.processing[account_id] = True
                    asyncio.create_task(self._process_batch(account_id))
    
    async def _process_batch(self, account_id: str):
        """Process a batch of requests for a specific account."""
        try:
            async with self.locks[account_id]:
                batch = self.batches[account_id]
                self.batches[account_id] = []
                self.last_processed[account_id] = time.time()
            
            if batch:
                reports = [item[0] for item in batch]
                bodies = [item[1] for item in batch]
                await store_webhook_data(reports, bodies, account_id)
                
                logging.info(f"Processed batch of {len(batch)} items for account {account_id}")
        except Exception as e:
            logging.exception(f"Error processing batch for account {account_id}: {e}")
        finally:
            self.processing[account_id] = False
    
    async def flush_all(self):
        """Flush all pending batches, typically called on shutdown."""
        tasks = []
        for account_id in list(self.batches.keys()):
            if self.batches[account_id]:
                tasks.append(self._process_batch(account_id))
        
        if tasks:
            await asyncio.gather(*tasks)

# Create a global batcher instance
webhook_batcher = WebhookBatcher(batch_size=50, max_wait_time=1.0)


class DashboardNotifier:
    """Optimized client for sending notifications to the dashboard."""
    
    def __init__(self, dashboard_url, max_concurrent=20, timeout=10.0):
        self.dashboard_url = dashboard_url
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=max_concurrent)
        )
        self.queue = asyncio.Queue()
        self.max_concurrent = max_concurrent
        self.workers = []
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    async def start(self):
        """Start the notification workers."""
        self.running = True
        for i in range(self.max_concurrent):
            worker = asyncio.create_task(self._worker())
            self.workers.append(worker)
        logging.info(f"Started {self.max_concurrent} dashboard notifier workers")
    
    async def stop(self):
        """Stop all workers and close the client."""
        self.running = False
        
        # Add None items to the queue to signal workers to stop
        for _ in range(len(self.workers)):
            await self.queue.put(None)
        
        if self.workers:
            await asyncio.gather(*self.workers)
        
        await self.client.aclose()
        self.executor.shutdown()
        logging.info("Dashboard notifier stopped")
    
    async def send_notification(self, response: Dict[str, Any]):
        """Queue a notification to be sent to the dashboard."""
        if self.running:
            await self.queue.put(response)
    
    async def _worker(self):
        """Worker process that sends notifications from the queue."""
        while self.running:
            response = await self.queue.get()
            if response is None:
                break
                
            try:
                # Extract message details from response to decide if we should notify
                should_notify = await self._should_notify(response)
                
                if should_notify:
                    await self._send_to_dashboard(response)
            except Exception as e:
                logging.error(f"Error processing dashboard notification: {e}")
            finally:
                self.queue.task_done()
    
    async def _should_notify(self, response: Dict[str, Any]) -> bool:
        """Determine if this response requires dashboard notification."""
        try:
            # Run the complex extraction logic in a thread to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor, 
                self._extract_notification_data, 
                response
            )
        except Exception as e:
            logging.error(f"Error determining notification status: {e}")
            return False
    
    def _extract_notification_data(self, response: Dict[str, Any]) -> bool:
        """Extract message text from response (runs in thread)."""
        try:
            # Complex extraction logic from your original code
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
            
            return bool(message_text or user_response or interactive_text)
        except Exception:
            return False
    
    async def _send_to_dashboard(self, response: Dict[str, Any]):
        """Send the notification to the dashboard."""
        try:
            dashboard_response = await self.client.post(
                self.dashboard_url, 
                json={'response': response},
                headers={'Content-Type': 'application/json'}
            )
            
            if dashboard_response.status_code == 200:
                logging.info("Response successfully sent to the dashboard")
            else:
                logging.warning(f"Failed to send response to dashboard. Status code: {dashboard_response.status_code}")
        except Exception as e:
            logging.error(f"Error sending to dashboard: {e}")
            
dashboard_notifier = DashboardNotifier(dashboard_url=DASHBOARD_URL)

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
                    interactive_type = message.get('interactive', {}).get('type')
                    if interactive_type == 'button_reply':
                        report['message_body'] = message.get('interactive', {}).get('button_reply', {}).get('title')
                    elif interactive_type == 'list_reply':
                        report['message_body'] = message.get('interactive', {}).get('list_reply', {}).get('title')
                    elif interactive_type == 'nfm_reply':
                        interactive_msg = message.get('interactive', {}).get('nfm_reply', {}).get('response_json')
                        type_msg = type(interactive_msg)
                        logging.info(f"interactive_msg {interactive_msg}")
                        logging.info(f"Type {type_msg}")
                        if isinstance(interactive_msg, str):
                            interactive_type_dict = json.loads(interactive_msg)
                        else:
                            interactive_type_dict = interactive_msg
                        report['message_body'] = clean_interactive_type(interactive_type_dict)
                        report['message_body'] = json.dumps(report['message_body'])
                else:
                    report['message_body'] = ""
    
    return report


async def store_webhook_data(reports, bodies, account_id):
    """Store multiple webhook data entries in the database at once."""
    connection = None
    cursor = None
    
    if not reports:
        return
        
    try:
        connection = get_connection()
        
        # Determine which table to use
        target_table = await create_account_table(account_id, connection)
        cursor = connection.cursor()
        
        # Prepare data for batch operations
        inserts = []
        insert_data = []
        updates = []
        
        # Check which records exist
        waba_ids = [report.get('waba_id') for report in reports if report.get('waba_id')]
        if not waba_ids:
            return
            
        placeholders = ', '.join(['%s'] * len(waba_ids))
        check_query = f"SELECT waba_id FROM {target_table} WHERE waba_id IN ({placeholders})"
        cursor.execute(check_query, waba_ids)
        existing_ids = {row[0] for row in cursor.fetchall()}
        
        # Prepare batch inserts and updates
        for i, report in enumerate(reports):
            waba_id = report.get('waba_id')
            if not waba_id:
                continue
                
            if waba_id not in existing_ids:
                inserts.append((
                    report.get('Date'),
                    report.get('display_phone_number'),
                    report.get('phone_number_id'),
                    waba_id,
                    report.get('contact_wa_id'),
                    report.get('status'),
                    report.get('message_timestamp'),
                    report.get('error_code'),
                    report.get('error_message'),
                    report.get('contact_name'),
                    report.get('message_from'),
                    report.get('message_type'),
                    report.get('message_body')
                ))
            else:
                updates.append((
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
                    waba_id
                ))
        
        # Execute batch insert if needed
        if inserts:
            batch_insert = (
                f"INSERT INTO {target_table} "
                "(Date, display_phone_number, phone_number_id, waba_id, contact_wa_id, status, "
                "message_timestamp, error_code, error_message, contact_name, message_from, "
                "message_type, message_body) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            cursor.executemany(batch_insert, inserts)
            
        # Execute batch update if needed
        if updates:
            batch_update = (
                f"UPDATE {target_table} SET "
                "Date = %s, display_phone_number = %s, phone_number_id = %s, contact_wa_id = %s, "
                "status = %s, message_timestamp = %s, error_code = %s, error_message = %s, "
                "contact_name = %s, message_from = %s, message_type = %s, message_body = %s "
                "WHERE waba_id = %s"
            )
            cursor.executemany(batch_update, updates)

        connection.commit()
        
        # Schedule dashboard notifications in the background only for new records
        for i, report in enumerate(reports):
            waba_id = report.get('waba_id')
            if waba_id and waba_id not in existing_ids:
                asyncio.create_task(send_to_dashboard(bodies[i]))

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
            
async def send_to_dashboard(response: Dict[str, Any]):
    """Queue a notification to be sent to the dashboard."""
    await dashboard_notifier.send_notification(response)