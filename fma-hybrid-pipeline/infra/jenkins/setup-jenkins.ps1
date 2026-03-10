# Jenkins Setup Script for Windows PowerShell
# This script helps set up Jenkins for the FMA Music Recommendation Pipeline

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Jenkins Setup for FMA Music Recommendation Pipeline" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is installed
try {
    docker --version | Out-Null
} catch {
    Write-Host "Error: Docker is not installed. Please install Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Check if Docker Compose is installed
try {
    docker-compose --version | Out-Null
} catch {
    Write-Host "Error: Docker Compose is not installed. Please install Docker Compose first." -ForegroundColor Red
    exit 1
}

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "Creating .env file from template..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host ""
    Write-Host "⚠️  IMPORTANT: Please edit the .env file with your actual credentials" -ForegroundColor Yellow
    Write-Host "   Location: $(Get-Location)\.env" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter after you've configured the .env file"
}

Write-Host "Building Jenkins Docker image..." -ForegroundColor Green
docker-compose build jenkins

Write-Host ""
Write-Host "Starting Jenkins and dependencies..." -ForegroundColor Green
docker-compose up -d

Write-Host ""
Write-Host "Waiting for Jenkins to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Get initial admin password
Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Jenkins is starting up!" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Jenkins UI: http://localhost:8080" -ForegroundColor Green
Write-Host "Kafka UI: http://localhost:8085" -ForegroundColor Green
Write-Host ""

# Try to get the initial password
try {
    $password = docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword 2>$null
    if ($password) {
        Write-Host "Initial Admin Password:" -ForegroundColor Yellow
        Write-Host $password -ForegroundColor White
        Write-Host ""
    }
} catch {
    Write-Host "Initial password not yet available. Please wait a moment and run:" -ForegroundColor Yellow
    Write-Host "  docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword" -ForegroundColor White
    Write-Host ""
}

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "1. Open http://localhost:8080 in your browser"
Write-Host "2. Paste the initial admin password shown above"
Write-Host "3. Install suggested plugins"
Write-Host "4. Create your admin user"
Write-Host "5. Configure credentials in Jenkins UI"
Write-Host "6. Create a new Pipeline job pointing to your Jenkinsfile"
Write-Host ""
Write-Host "For detailed instructions, see: $(Get-Location)\README.md" -ForegroundColor Yellow
Write-Host ""
Write-Host "To view logs: docker logs -f jenkins" -ForegroundColor Cyan
Write-Host "To stop: docker-compose down" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
