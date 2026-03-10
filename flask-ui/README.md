# Flask Music Recommendation UI

A web-based music recommendation system powered by KMeans clustering on audio features extracted from music tracks. This Flask application integrates with Databricks for data access and MLflow for model serving.

## Features

- 🎵 **Music Library Browser** - Browse and play music tracks organized by clusters
- 🔊 **Audio Upload & Classification** - Upload audio files for real-time cluster prediction
- 🤖 **AI-Powered Recommendations** - Get similar tracks based on audio feature clustering
- ⭐ **Favorites Management** - Save and manage your favorite tracks
- 📊 **Cluster Statistics** - View distribution of tracks across clusters
- 🎨 **Modern UI** - Responsive design with drag-and-drop upload

## Architecture

```
User Browser
    ↓
Flask App (app.py)
    ├── Databricks SQL Connector → Query gold/silver/bronze tables
    ├── MLflow → Load KMeans model for predictions
    ├── Librosa → Extract audio features (tempo, MFCC, spectral centroid)
    └── AWS S3 → Serve audio files via presigned URLs
```

## Prerequisites

1. **Python 3.10+** installed
2. **Databricks Workspace** with:
   - SQL Warehouse running
   - Tables: `fma_catalog.bronze.audio_raw`, `fma_catalog.silver.audio_unsupervised`, `fma_catalog.gold.audio_clusters`
   - MLflow model trained and registered
3. **AWS S3** bucket with audio files
4. **Access credentials** for Databricks and AWS

## Installation

### 1. Clone and Navigate

```bash
cd flask-ui
```

### 2. Create Virtual Environment (Recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r ../fma-hybrid-pipeline/requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file based on the template:

```bash
cp .env.example .env
```

Edit `.env` and fill in your credentials:

```env
# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=dapi...your-token-here
DATABRICKS_CATALOG=fma_catalog

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
S3_BUCKET_NAME=fma-data-bucket

# MLflow Configuration (optional - defaults to Databricks)
MLFLOW_TRACKING_URI=databricks
MLFLOW_MODEL_URI=models:/kmeans_music_clustering/Production
```

### 5. Get Databricks Credentials

**Server Hostname & HTTP Path:**
1. Go to your Databricks workspace
2. Navigate to: **SQL Warehouses**
3. Click on your warehouse
4. Go to **Connection Details** tab
5. Copy `Server hostname` and `HTTP path`

**Access Token:**
1. Go to: **Settings** → **User Settings** → **Developer** (or **Access Tokens**)
2. Click **Generate New Token**
3. Give it a name (e.g., "Flask App") and set expiration
4. Copy the token (shown only once!)

## Running the Application

### Development Server

```bash
python run.py
```

The app will start on `http://localhost:5000`

### Production Server (Gunicorn)

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## Project Structure

```
flask-ui/
├── app.py                      # Main Flask application with API routes
├── run.py                      # Entry point script
├── config.py                   # Configuration management
├── .env                        # Environment variables (create from .env.example)
├── .env.example                # Environment template
├── services/                   # Service layer
│   ├── __init__.py
│   ├── databricks_service.py  # Databricks SQL queries
│   ├── model_service.py       # MLflow model loading/inference
│   └── audio_processor.py     # Audio feature extraction (librosa)
├── templates/                  # HTML templates (Jinja2)
│   ├── base.html
│   └── index.html
└── static/                     # Frontend assets
    ├── css/
    │   └── style.css
    ├── js/
    │   └── main.js
    └── uploads/                # Temporary upload storage
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Render main UI |
| `/api/tracks` | GET | Get all tracks with cluster assignments |
| `/api/tracks/<id>` | GET | Get single track by ID |
| `/api/upload` | POST | Upload audio file for classification |
| `/api/recommendations/<id>` | GET | Get similar tracks (same cluster) |
| `/api/cluster-stats` | GET | Get cluster distribution statistics |
| `/api/favorites` | GET/POST/DELETE | Manage favorite tracks |

## Feature Extraction

Audio features extracted (matching Databricks Silver notebook):

1. **Tempo** - BPM (beats per minute)
2. **MFCC** - 13 Mel-Frequency Cepstral Coefficients (means)
3. **Spectral Centroid** - Mean spectral centroid (brightness)

**Total: 15 features** → Normalized with StandardScaler → KMeans clustering (K=2)

## Troubleshooting

### Configuration Validation Error

```bash
python config.py
```

This will test if all environment variables are loaded correctly.

### Databricks Connection Issues

- Verify SQL Warehouse is **running** (not stopped)
- Check token hasn't expired
- Ensure network access to Databricks workspace
- Test connection manually:

```python
from databricks import sql
connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/your-id",
    access_token="your-token"
)
cursor = connection.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())
```

### MLflow Model Loading Issues

- Check if model exists in MLflow registry
- Verify model URI in `.env` is correct
- Try alternative URIs:
  - `models:/model_name/Production` (registered model)
  - `runs:/run-id/artifact-path` (specific run)
  - `models:/model_name/1` (specific version)

### Audio Processing Errors

- Ensure `librosa` and `soundfile` are installed correctly
- Check audio file format (supports: mp3, wav, ogg)
- Verify file size is under 50MB

### S3 Access Issues

- Check AWS credentials are correct
- Verify S3 bucket name matches
- Ensure bucket region matches `AWS_REGION`
- Test S3 access:

```python
import boto3
s3 = boto3.client('s3', region_name='us-east-1')
s3.list_objects_v2(Bucket='your-bucket', MaxKeys=1)
```

## Development

### Running Tests

```bash
# Test configuration
python config.py

# Test Databricks connection
python -c "from services.databricks_service import DatabricksService; from config import config; db = DatabricksService(config); db.connect(); print('✓ Connected')"

# Test audio processing
python -c "from services.audio_processor import AudioProcessor; from config import config; ap = AudioProcessor(config); print('✓ Audio processor ready')"
```

### Debug Mode

Set in `.env`:
```env
FLASK_DEBUG=True
```

This enables:
- Auto-reload on code changes
- Detailed error pages
- Interactive debugger

## License

MIT License

## Support

For issues related to:
- **Databricks integration**: Check Databricks SQL Connector docs
- **MLflow models**: Check MLflow documentation
- **Audio processing**: Check librosa documentation
- **Flask app**: Check Flask documentation
