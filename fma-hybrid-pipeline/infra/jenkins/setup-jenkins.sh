#!/bin/bash
# Jenkins Setup Script
# This script helps set up Jenkins for the FMA Music Recommendation Pipeline

set -e

echo "================================================"
echo "Jenkins Setup for FMA Music Recommendation Pipeline"
echo "================================================"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Please edit the .env file with your actual credentials"
    echo "   Location: $(pwd)/.env"
    echo ""
    read -p "Press Enter after you've configured the .env file..."
fi

# Load environment variables
source .env

echo "Building Jenkins Docker image..."
docker-compose build jenkins

echo ""
echo "Starting Jenkins and dependencies..."
docker-compose up -d

echo ""
echo "Waiting for Jenkins to start..."
sleep 30

# Get initial admin password
echo ""
echo "================================================"
echo "Jenkins is starting up!"
echo "================================================"
echo ""
echo "Jenkins UI: http://localhost:8080"
echo "Kafka UI: http://localhost:8085"
echo ""

# Try to get the initial password
if docker exec jenkins test -f /var/jenkins_home/secrets/initialAdminPassword; then
    echo "Initial Admin Password:"
    docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
    echo ""
else
    echo "Initial password not yet available. Please wait a moment and run:"
    echo "  docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword"
    echo ""
fi

echo "================================================"
echo "Next Steps:"
echo "================================================"
echo "1. Open http://localhost:8080 in your browser"
echo "2. Paste the initial admin password shown above"
echo "3. Install suggested plugins"
echo "4. Create your admin user"
echo "5. Configure credentials in Jenkins UI"
echo "6. Create a new Pipeline job pointing to your Jenkinsfile"
echo ""
echo "For detailed instructions, see: $(pwd)/README.md"
echo ""
echo "To view logs: docker logs -f jenkins"
echo "To stop: docker-compose down"
echo "================================================"
