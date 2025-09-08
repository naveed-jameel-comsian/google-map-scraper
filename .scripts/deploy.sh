#!/bin/bash
set -e

echo "🚀 Deployment started ..."

# Move to the project folder directory
echo "📂 Moving to project folder directory"
cd /home/ubuntu/scraper-project

# Pull the latest version of the app
echo "📥 Pulling latest changes..."
git pull origin main
echo "✅ New changes copied to server!"

# Activate Virtual Env
source venv/bin/activate
echo "Virtual env 'venv' activated!"

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Deactivate Virtual Env
deactivate
echo "Virtual env 'venv' deactivated!"

# Reload services
echo "Restarting Nginx & Gunicorn..."
sudo systemctl daemon-reload
sudo systemctl restart nginx
sudo systemctl restart fastapi
echo "✅ Services restarted!"

echo "Deployment Finished!"