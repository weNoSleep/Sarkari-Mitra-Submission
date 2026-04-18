#!/bin/bash
# deploy.sh — Run this once to set up Cloudflare Tunnel
# No port forwarding, no ngrok account needed

echo "=== Sarkari-Mitra WhatsApp Bot Deploy ==="

# Step 1: Install cloudflared
if ! command -v cloudflared &> /dev/null; then
    echo "Installing cloudflared..."
    # Linux
    wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
    sudo dpkg -i cloudflared-linux-amd64.deb
    # Mac: brew install cloudflare/cloudflare/cloudflared
fi

# Step 2: Install Python deps
pip install -r requirements.txt

# Step 3: Start FastAPI in background
echo "Starting FastAPI server..."
uvicorn main:app --host 0.0.0.0 --port 8000 &
FASTAPI_PID=$!
echo "FastAPI PID: $FASTAPI_PID"
trap "kill $FASTAPI_PID; exit" INT TERM EXIT
sleep 3

# Step 4: Start Cloudflare tunnel
echo "Starting Cloudflare tunnel..."
echo "Copy the https URL that appears below"
echo "Paste it in Twilio → Messaging → WhatsApp Sandbox → Webhook URL"
echo "Add /webhook at the end"
echo ""
cloudflared tunnel --url http://localhost:8000