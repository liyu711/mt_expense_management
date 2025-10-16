@echo off
REM Build and run the app_local Docker image for Windows (PowerShell/Command Prompt)
SET IMAGE_NAME=mt-expense-app-local
nSET CONTAINER_NAME=mt-expense-app-local

echo Checking Docker availability...
docker version >nul 2>&1
if ERRORLEVEL 1 (
    echo Docker does not appear to be running or is not on PATH.
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo Building Docker image %IMAGE_NAME% from app_local/Dockerfile...
docker build -t %IMAGE_NAME% -f .\app_local\Dockerfile .
if ERRORLEVEL 1 (
    echo Docker build failed. Check the output above for errors.
    pause
    exit /b 1
)

echo Stopping any running container named %CONTAINER_NAME%...
docker ps -q -f name=%CONTAINER_NAME% >nul 2>&1 && (
    docker stop %CONTAINER_NAME% >nul 2>&1
    docker rm %CONTAINER_NAME% >nul 2>&1
)

echo Running container %CONTAINER_NAME% (publishing port 8000)...
docker run -d --name %CONTAINER_NAME% -p 8000:8000 -e FLASK_ENV=development %IMAGE_NAME%
if ERRORLEVEL 1 (
    echo Failed to start container. Check Docker logs.
    pause
    exit /b 1
)

echo Waiting 2 seconds for the container to start...
timeout /t 2 >nul

echo Opening http://localhost:8000 in your default browser...
start http://localhost:8000

echo Done. To view logs, run: docker logs -f %CONTAINER_NAME%
pause
