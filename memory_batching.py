import asyncio
import time
import httpx
from collections import defaultdict
from typing import Dict, Any
from utils import logging, DASHBOARD_URL
from parse_and_store_webhook_data import store_webhook_data
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

async def send_to_dashboard(response: Dict[str, Any]):
    """Queue a notification to be sent to the dashboard."""
    await dashboard_notifier.send_notification(response)