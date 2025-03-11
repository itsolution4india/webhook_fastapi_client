from typing import Dict, Any
from utils import logging
import uvicorn
from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from memory_batching import webhook_batcher, dashboard_notifier
from parse_and_store_webhook_data import parse_webhook_response, store_webhook_data

# Create FastAPI app

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting FastAPI application")
    await dashboard_notifier.start()
    yield
    logging.info("Shutting down FastAPI application")
    await webhook_batcher.flush_all()
    await dashboard_notifier.stop()
    
app = FastAPI(lifespan=lifespan)

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