"""
Flask Music Recommendation API
Main application with all API endpoints for music discovery and recommendations
"""
from flask import Flask, render_template, request, jsonify, session, send_from_directory
from werkzeug.utils import secure_filename
import os
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid

# Import configuration
from config import config

# Import services
from services.databricks_service import DatabricksService
from services.model_service import ModelService
from services.audio_processor import AudioProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = config.SECRET_KEY
app.config['MAX_CONTENT_LENGTH'] = config.MAX_CONTENT_LENGTH
app.config['UPLOAD_FOLDER'] = config.UPLOAD_FOLDER

# Ensure upload folder exists
os.makedirs(config.UPLOAD_FOLDER, exist_ok=True)

# Initialize services
databricks_service = DatabricksService(config)
model_service = ModelService(config)
audio_processor = AudioProcessor(config)

# Initialize S3 client for presigned URLs
s3_client = None
if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION
    )
    logger.info("S3 client initialized")


def allowed_file(filename: str) -> bool:
    """Check if filename has allowed extension"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in config.ALLOWED_EXTENSIONS


def generate_presigned_url(s3_path: str) -> str:
    """Generate presigned URL for S3 object
    
    Args:
        s3_path: Full S3 path (e.g., s3://bucket/key or just key)
        
    Returns:
        Presigned URL or original path if S3 not configured
    """
    if not s3_client:
        logger.warning("S3 client not configured, returning original path")
        return s3_path
    
    try:
        # Extract key from S3 path
        if s3_path.startswith('s3://'):
            # Format: s3://bucket/key
            parts = s3_path.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ''
        else:
            # Assume it's just the key
            bucket = config.S3_BUCKET_NAME
            key = s3_path.replace(config.S3_AUDIO_PREFIX, '', 1)
        
        # Generate presigned URL
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=config.PRESIGNED_URL_EXPIRATION
        )
        
        return url
        
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        return s3_path


# Initialize services on first request
@app.before_request
def initialize_services():
    """Initialize services on first request"""
    if not hasattr(app, 'services_initialized'):
        try:
            logger.info("Initializing services...")
            
            # Validate configuration
            config.validate()
            
            # Connect to Databricks
            databricks_service.connect()
            logger.info("Connected to Databricks")
            
            # Load ML model
            try:
                model_service.load_model()
                if config.USE_MODEL_SERVING:
                    logger.info("Using Databricks Model Serving for predictions")
                else:
                    logger.info("Loaded ML model from MLflow")
            except Exception as e:
                logger.error(f"Failed to load model: {e}")
                if not config.USE_MODEL_SERVING:
                    logger.info("Will attempt to compute scaler from Silver layer")
            
            # Compute StandardScaler if not loaded from MLflow
            if model_service.scaler is None:
                logger.info("Computing StandardScaler from Silver layer...")
                success = model_service.compute_scaler_from_silver(databricks_service)
                if not success:
                    logger.warning("Using raw features without normalization")
            
            # Initialize session favorites
            if 'favorites' not in session:
                session['favorites'] = []
            
            app.services_initialized = True
            logger.info("Services initialized successfully")
            
        except Exception as e:
            logger.error(f"Service initialization failed: {e}")
            # Don't prevent app from starting, but log the error


# ============================================================================
# WEB ROUTES
# ============================================================================

@app.route('/')
def index():
    """Render main page"""
    return render_template('index.html')


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/api/tracks', methods=['GET'])
def get_tracks():
    """Get all tracks with cluster assignments
    
    Returns:
        JSON object with tracks:
        {
            "success": true,
            "tracks": [...],
            "count": 6015
        }
    """
    try:
        # Get cluster filter from query params
        cluster_id = request.args.get('cluster_id', type=int)
        
        # Query tracks
        if cluster_id is not None:
            tracks = databricks_service.get_tracks_by_cluster(cluster_id, limit=1000)
        else:
            tracks = databricks_service.get_all_tracks()
        
        # Add presigned URLs
        for track in tracks:
            track['url'] = generate_presigned_url(track['path'])
        
        logger.info(f"Returning {len(tracks)} tracks")
        return jsonify({
            "success": True,
            "tracks": tracks,
            "count": len(tracks)
        })
        
    except Exception as e:
        logger.error(f"Failed to get tracks: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tracks/<int:track_id>', methods=['GET'])
def get_track(track_id: int):
    """Get single track by ID
    
    Args:
        track_id: Track identifier
        
    Returns:
        JSON track object or 404
    """
    try:
        track = databricks_service.get_track_by_id(track_id)
        
        if not track:
            return jsonify({"error": "Track not found"}), 404
        
        # Add presigned URL
        track['url'] = generate_presigned_url(track['path'])
        
        return jsonify(track)
        
    except Exception as e:
        logger.error(f"Failed to get track {track_id}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/upload', methods=['POST'])
def upload_audio():
    """Upload audio file, extract features, and predict cluster
    
    Request:
        Form data with 'file' field containing audio file
        
    Returns:
        JSON with cluster prediction and metadata:
        {
            "cluster_id": 0,
            "filename": "uploaded.mp3",
            "features": {...},
            "message": "Success"
        }
    """
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                "error": f"Invalid file type. Allowed: {', '.join(config.ALLOWED_EXTENSIONS)}"
            }), 400
        
        # Save file with unique name
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(config.UPLOAD_FOLDER, unique_filename)
        file.save(file_path)
        
        logger.info(f"Saved uploaded file: {unique_filename}")
        
        # Process audio file
        cluster_id, metadata = audio_processor.process_upload(file_path, model_service)
        
        # Clean up uploaded file
        try:
            os.remove(file_path)
        except:
            pass
        
        if cluster_id is None:
            return jsonify(metadata), 400
        
        # Return success response
        response = {
            "cluster_id": cluster_id,
            "filename": filename,
            "message": "Audio file processed successfully",
            **metadata
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Upload processing failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/recommendations/<int:track_id>', methods=['GET'])
def get_recommendations(track_id: int):
    """Get recommended tracks (tracks in same cluster)
    
    Args:
        track_id: Reference track ID
        
    Returns:
        JSON object with recommendations:
        {
            "success": true,
            "recommendations": [...],
            "count": 10,
            "cluster_id": 0
        }
    """
    try:
        # Get the reference track to find its cluster
        track = databricks_service.get_track_by_id(track_id)
        
        if not track:
            return jsonify({"success": False, "error": "Track not found"}), 404
        
        cluster_id = track['cluster_id']
        
        # Get limit from query params (default 10)
        limit = request.args.get('limit', default=10, type=int)
        
        # Get tracks in same cluster, excluding the reference track
        recommendations = databricks_service.get_tracks_by_cluster(
            cluster_id,
            exclude_track_id=track_id,
            limit=limit
        )
        
        # Add presigned URLs
        for rec in recommendations:
            rec['url'] = generate_presigned_url(rec['path'])
        
        logger.info(f"Returning {len(recommendations)} recommendations for track {track_id}")
        return jsonify({
            "success": True,
            "recommendations": recommendations,
            "count": len(recommendations),
            "cluster_id": cluster_id
        })
        
    except Exception as e:
        logger.error(f"Failed to get recommendations for track {track_id}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/cluster-stats', methods=['GET'])
def get_cluster_stats():
    """Get statistics on cluster distribution
    
    Returns:
        JSON with cluster statistics:
        {
            "success": true,
            "total_tracks": 1000,
            "cache_stats": {"0": 600, "1": 400},
            "clusters": [
                {"cluster_id": 0, "count": 600, "percentage": 60.0},
                {"cluster_id": 1, "count": 400, "percentage": 40.0}
            ]
        }
    """
    try:
        stats = databricks_service.get_cluster_stats()
        
        # Calculate total and percentages
        total_tracks = sum(s['count'] for s in stats)
        
        clusters = []
        cache_stats = {}
        for stat in stats:
            clusters.append({
                "cluster_id": stat['cluster_id'],
                "count": stat['count'],
                "percentage": round((stat['count'] / total_tracks * 100), 2) if total_tracks > 0 else 0
            })
            cache_stats[str(stat['cluster_id'])] = stat['count']
        
        response = {
            "success": True,
            "total_tracks": total_tracks,
            "cache_stats": cache_stats,
            "clusters": clusters
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Failed to get cluster stats: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/favorites', methods=['GET'])
def get_favorites():
    """Get user's favorite tracks
    
    Returns:
        JSON array of favorite track IDs
    """
    favorites = session.get('favorites', [])
    return jsonify(favorites)


@app.route('/api/favorites', methods=['POST'])
def add_favorite():
    """Add track to favorites
    
    Request body:
        {"track_id": 123}
        
    Returns:
        Success message and updated favorites list
    """
    try:
        data = request.get_json()
        track_id = data.get('track_id')
        
        if track_id is None:
            return jsonify({"success": False, "error": "track_id required"}), 400
        
        # Get or initialize favorites
        if 'favorites' not in session:
            session['favorites'] = []
        
        favorites = session['favorites']
        
        # Add if not already present
        if track_id not in favorites:
            favorites.append(track_id)
            session['favorites'] = favorites
            session.modified = True
            logger.info(f"Added track {track_id} to favorites")
        
        return jsonify({
            "success": True,
            "message": "Added to favorites",
            "favorites": favorites
        })
        
    except Exception as e:
        logger.error(f"Failed to add favorite: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/favorites', methods=['DELETE'])
def remove_favorite():
    """Remove track from favorites
    
    Query params:
        track_id: Track ID to remove
        
    Returns:
        Success message and updated favorites list
    """
    try:
        # Get track_id from query params (DELETE requests typically use query params)
        track_id = request.args.get('track_id', type=int)
        
        if track_id is None:
            return jsonify({"success": False, "error": "track_id required"}), 400
        
        # Get favorites
        favorites = session.get('favorites', [])
        
        # Remove if present
        if track_id in favorites:
            favorites.remove(track_id)
            session['favorites'] = favorites
            session.modified = True
            logger.info(f"Removed track {track_id} from favorites")
        
        return jsonify({
            "success": True,
            "message": "Removed from favorites",
            "favorites": favorites
        })
        
    except Exception as e:
        logger.error(f"Failed to remove favorite: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle file too large errors"""
    return jsonify({"error": "File too large (max 50MB)"}), 413


# ============================================================================
# CLEANUP
# ============================================================================

@app.teardown_appcontext
def cleanup(error=None):
    """Cleanup resources on app shutdown"""
    # Don't close the connection after each request
    # The connection will be reused across requests
    # Only close on actual app shutdown
    pass


# Proper shutdown handler
import atexit

@atexit.register
def shutdown():
    """Close connections on application exit"""
    try:
        logger.info("Shutting down, closing Databricks connection...")
        databricks_service.close()
    except:
        pass


if __name__ == '__main__':
    app.run(
        host=config.HOST,
        port=config.PORT,
        debug=config.DEBUG
    )
