"""
Configuration management for Flask Music Recommendation App
Loads environment variables and provides configuration objects
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Base configuration class"""
    
    # Flask settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    PORT = int(os.getenv('FLASK_PORT', '5000'))
    
    # Databricks configuration
    DATABRICKS_SERVER_HOSTNAME = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
    DATABRICKS_ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')
    DATABRICKS_CATALOG = os.getenv('DATABRICKS_CATALOG', 'fma_catalog')
    
    # MLflow configuration
    MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'databricks')
    MLFLOW_MODEL_URI = os.getenv('MLFLOW_MODEL_URI', 'models:/kmeans_music_clustering/Production')
    
    # Databricks Model Serving configuration
    MODEL_SERVING_ENDPOINT = os.getenv('MODEL_SERVING_ENDPOINT')  # e.g., 'kmeans_music_clustering'
    USE_MODEL_SERVING = os.getenv('USE_MODEL_SERVING', 'true').lower() == 'true'
    
    # AWS S3 configuration for audio files
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'fma-data-bucket')
    S3_AUDIO_PREFIX = os.getenv('S3_AUDIO_PREFIX', 'raw_audio/')
    PRESIGNED_URL_EXPIRATION = int(os.getenv('PRESIGNED_URL_EXPIRATION', '3600'))  # 1 hour
    
    # Audio processing settings
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'static/uploads')
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB max file size
    ALLOWED_EXTENSIONS = {'mp3', 'wav', 'ogg'}
    AUDIO_SAMPLE_RATE = 22050
    AUDIO_DURATION = 30.0  # Process first 30 seconds (matching Silver notebook)
    
    # Feature extraction settings (matching Silver notebook)
    N_MFCC = 13
    N_CHROMA = 12
    N_SPECTRAL_CONTRAST_BANDS = 7
    # Feature count: 1 tempo + 1 beat_strength + 26 mfcc + 2 spectral_centroid + 24 chroma + 
    #                2 spectral_rolloff + 2 spectral_bandwidth + 14 spectral_contrast + 2 zcr = 74
    FEATURE_COUNT = 74
    
    @classmethod
    def validate(cls):
        """Validate required configuration variables"""
        required_vars = [
            'DATABRICKS_SERVER_HOSTNAME',
            'DATABRICKS_HTTP_PATH',
            'DATABRICKS_ACCESS_TOKEN',
        ]
        
        missing = []
        for var in required_vars:
            if not getattr(cls, var):
                missing.append(var)
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                f"Please create a .env file based on .env.example"
            )
        
        return True
    
    @classmethod
    def get_table_name(cls, layer, table):
        """Get fully qualified table name"""
        return f"{cls.DATABRICKS_CATALOG}.{layer}.{table}"


# Configuration instance
config = Config()


if __name__ == '__main__':
    # Test configuration loading
    try:
        config.validate()
        print("✓ Configuration loaded successfully")
        print(f"  - Databricks Host: {config.DATABRICKS_SERVER_HOSTNAME}")
        print(f"  - Catalog: {config.DATABRICKS_CATALOG}")
        print(f"  - S3 Bucket: {config.S3_BUCKET_NAME}")
        print(f"  - MLflow URI: {config.MLFLOW_TRACKING_URI}")
    except ValueError as e:
        print(f"✗ Configuration error: {e}")
