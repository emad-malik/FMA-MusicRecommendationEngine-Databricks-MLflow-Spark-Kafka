"""
Entry point for Flask Music Recommendation Application
Run this script to start the development server
"""
from app import app, config, logger
import sys


def main():
    """Main entry point"""
    try:
        logger.info("="*60)
        logger.info("Flask Music Recommendation System")
        logger.info("="*60)
        logger.info(f"Starting server on {config.HOST}:{config.PORT}")
        logger.info(f"Debug mode: {config.DEBUG}")
        logger.info(f"Databricks: {config.DATABRICKS_SERVER_HOSTNAME}")
        logger.info(f"S3 Bucket: {config.S3_BUCKET_NAME}")
        logger.info("="*60)
        
        # Validate configuration
        try:
            config.validate()
            logger.info("✓ Configuration validated")
        except ValueError as e:
            logger.error(f"✗ Configuration error: {e}")
            logger.error("Please check your .env file and ensure all required variables are set")
            sys.exit(1)
        
        # Start Flask app
        app.run(
            host=config.HOST,
            port=config.PORT,
            debug=config.DEBUG,
            use_reloader=config.DEBUG  # Auto-reload in debug mode
        )
        
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
