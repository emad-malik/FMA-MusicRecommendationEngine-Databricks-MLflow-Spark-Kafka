# Pushes FMA metadata events to Kafka after uploading audio files to S3.


import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FMAProducerConfig:
    """Configuration management for the FMA producer."""
    
    def __init__(self):
        load_dotenv()
        
        # AWS Configuration
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.s3_bucket = os.getenv('AWS_S3_BUCKET')
        
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'audio_uploads')
        
        # Producer Configuration
        self.sample_size = int(os.getenv('SAMPLE_SIZE', '50'))
        self.fma_metadata_path = Path(os.getenv('FMA_METADATA_PATH', 'data/fma_metadata'))
        self.fma_audio_path = Path(os.getenv('FMA_AUDIO_PATH', 'data/fma_medium'))
        
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.aws_access_key:
            errors.append("AWS_ACCESS_KEY_ID not set")
        if not self.aws_secret_key:
            errors.append("AWS_SECRET_ACCESS_KEY not set")
        if not self.s3_bucket:
            errors.append("AWS_S3_BUCKET not set")
        if not self.fma_metadata_path.exists():
            errors.append(f"Metadata path not found: {self.fma_metadata_path}")
        if not self.fma_audio_path.exists():
            errors.append(f"Audio path not found: {self.fma_audio_path}")
            
        return errors


class MetadataLoader:
    """Loads and processes FMA metadata from CSV files."""
    
    def __init__(self, metadata_path: Path):
        self.metadata_path = metadata_path
        self.tracks_df = None
        self.genres_df = None
        
    def load(self) -> None:
        """Load tracks and genres metadata."""
        logger.info("Loading FMA metadata...")
        
        # Load tracks with multi-level headers
        tracks_file = self.metadata_path / 'tracks.csv'
        self.tracks_df = pd.read_csv(tracks_file, header=[0, 1], low_memory=False)
        logger.info(f"Loaded {len(self.tracks_df)} tracks from metadata")
        
        # Filter to only 'medium' subset tracks
        try:
            subset_col = ('set', 'subset')
            if subset_col in self.tracks_df.columns:
                self.tracks_df = self.tracks_df[self.tracks_df[subset_col] == 'medium']
                logger.info(f"Filtered to {len(self.tracks_df)} tracks in 'medium' subset")
        except Exception as e:
            logger.warning(f"Could not filter by subset: {e}")
        
        # Load genres
        genres_file = self.metadata_path / 'genres.csv'
        self.genres_df = pd.read_csv(genres_file)
        logger.info(f"Loaded {len(self.genres_df)} genres")
        
    def sample_tracks(self, n: int) -> pd.DataFrame:
        """Sample N random tracks and enrich with genre information."""
        if self.tracks_df is None:
            raise ValueError("Metadata not loaded. Call load() first.")
        
        # Sample tracks
        sampled = self.tracks_df.sample(n=min(n, len(self.tracks_df)))
        logger.info(f"Sampled {len(sampled)} tracks")
        
        return sampled
    
    def extract_metadata(self, track_row) -> Dict:
        """Extract relevant metadata from a track row."""
        try:
            # Handle multi-level column structure (use tuple indexing)
            # The row index is the track_id
            track_id = int(track_row.name)
            
            metadata = {
                'track_id': track_id,
            }
            
            # Extract track information (safely handle missing columns)
            try:
                metadata['title'] = str(track_row[('track', 'title')]) if pd.notna(track_row.get(('track', 'title'))) else 'Unknown'
                metadata['duration'] = float(track_row[('track', 'duration')]) if pd.notna(track_row.get(('track', 'duration'))) else 0.0
            except (KeyError, IndexError, ValueError):
                metadata['title'] = 'Unknown'
                metadata['duration'] = 0.0
            
            # Extract album information
            try:
                metadata['album_title'] = str(track_row[('album', 'title')]) if pd.notna(track_row.get(('album', 'title'))) else 'Unknown'
            except (KeyError, IndexError):
                metadata['album_title'] = 'Unknown'
            
            # Extract artist information
            try:
                metadata['artist_name'] = str(track_row[('artist', 'name')]) if pd.notna(track_row.get(('artist', 'name'))) else 'Unknown'
            except (KeyError, IndexError):
                metadata['artist_name'] = 'Unknown'
            
            # Extract genre information
            try:
                genre_top = track_row[('track', 'genre_top')]
                metadata['genre_top'] = str(genre_top) if pd.notna(genre_top) else 'Unknown'
            except (KeyError, IndexError):
                metadata['genre_top'] = 'Unknown'
            
            return metadata
            
        except Exception as e:
            logger.warning(f"Error extracting metadata for track {track_row.name}: {e}")
            return {
                'track_id': int(track_row.name),
                'title': 'Unknown',
                'duration': 0.0,
                'album_title': 'Unknown',
                'artist_name': 'Unknown',
                'genre_top': 'Unknown'
            }


class AudioFileResolver:
    """Resolves track IDs to audio file paths."""
    
    def __init__(self, audio_path: Path):
        self.audio_path = audio_path
        
    def resolve(self, track_id: int) -> Optional[Path]:
        """Convert track_id to file path and verify existence."""
        # Calculate folder (e.g., track_id 2 -> folder "000")
        folder = f"{track_id // 1000:03d}"
        
        # Build file path
        filename = f"{track_id:06d}.mp3"
        file_path = self.audio_path / folder / filename
        
        if file_path.exists():
            return file_path
        else:
            logger.warning(f"Audio file not found: {file_path}")
            return None


class S3Uploader:
    """Uploads audio files to AWS S3."""
    
    def __init__(self, config: FMAProducerConfig):
        self.config = config
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key,
            region_name=config.aws_region
        )
        
    def upload(self, local_path: Path, track_id: int) -> Optional[str]:
        """Upload audio file to S3 and return S3 URI."""
        s3_key = f"raw_audio/{track_id:06d}.mp3"
        
        try:
            logger.info(f"Uploading {local_path.name} to s3://{self.config.s3_bucket}/{s3_key}")
            
            self.s3_client.upload_file(
                str(local_path),
                self.config.s3_bucket,
                s3_key,
                ExtraArgs={'ContentType': 'audio/mpeg'}
            )
            
            s3_uri = f"s3://{self.config.s3_bucket}/{s3_key}"
            logger.info(f"Successfully uploaded to {s3_uri}")
            return s3_uri
            
        except ClientError as e:
            logger.error(f"Failed to upload {local_path.name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error uploading {local_path.name}: {e}")
            return None


class KafkaMessagePublisher:
    """Publishes messages to Kafka."""
    
    def __init__(self, config: FMAProducerConfig):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        
    def publish(self, track_id: int, metadata: Dict, s3_uri: str) -> bool:
        """Publish message to Kafka topic."""
        message = {
            'track_id': track_id,
            'metadata': metadata,
            's3_uri': s3_uri,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        try:
            future = self.producer.send(
                self.config.kafka_topic,
                key=track_id,
                value=message
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            logger.info(f"Published message for track {track_id} to {self.config.kafka_topic} "
                       f"(partition {record_metadata.partition}, offset {record_metadata.offset})")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish message for track {track_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing message for track {track_id}: {e}")
            return False
            
    def close(self):
        """Close the Kafka producer."""
        self.producer.flush()
        self.producer.close()


class FMAProducer:
    """Main orchestrator for the FMA audio upload pipeline."""
    
    def __init__(self, config: FMAProducerConfig):
        self.config = config
        self.metadata_loader = MetadataLoader(config.fma_metadata_path)
        self.file_resolver = AudioFileResolver(config.fma_audio_path)
        self.s3_uploader = S3Uploader(config)
        self.kafka_publisher = KafkaMessagePublisher(config)
        
        self.stats = {
            'total': 0,
            'uploaded': 0,
            'published': 0,
            'failed': 0,
            'skipped': 0
        }
        
    def process_track(self, track_row) -> bool:
        """Process a single track: extract metadata, upload to S3, publish to Kafka."""
        self.stats['total'] += 1
        
        # Extract metadata
        metadata = self.metadata_loader.extract_metadata(track_row)
        track_id = metadata['track_id']
        
        logger.info(f"Processing track {track_id}: {metadata.get('artist_name')} - {metadata.get('title')}")
        
        # Resolve audio file path
        audio_path = self.file_resolver.resolve(track_id)
        if audio_path is None:
            logger.warning(f"Skipping track {track_id}: audio file not found")
            self.stats['skipped'] += 1
            return False
        
        # Upload to S3
        s3_uri = self.s3_uploader.upload(audio_path, track_id)
        if s3_uri is None:
            logger.error(f"Failed to upload track {track_id}")
            self.stats['failed'] += 1
            return False
        
        self.stats['uploaded'] += 1
        
        # Publish to Kafka
        if self.kafka_publisher.publish(track_id, metadata, s3_uri):
            self.stats['published'] += 1
            return True
        else:
            self.stats['failed'] += 1
            return False
    
    def run(self, sample_size: Optional[int] = None, dry_run: bool = False) -> None:
        """Run the producer pipeline."""
        try:
            # Load metadata
            self.metadata_loader.load()
            
            # Sample tracks with file verification
            size = sample_size if sample_size is not None else self.config.sample_size
            logger.info(f"Attempting to find {size} tracks with audio files...")
            
            # Over-sample and filter to tracks with actual files
            tracks_with_files = []
            attempts = 0
            max_attempts = size * 10  # Try up to 10x the requested size
            
            while len(tracks_with_files) < size and attempts < max_attempts:
                # Sample a batch
                batch_size = min((size - len(tracks_with_files)) * 2, 100)
                sampled_batch = self.metadata_loader.sample_tracks(batch_size)
                
                # Check which have files
                for idx, row in sampled_batch.iterrows():
                    if len(tracks_with_files) >= size:
                        break
                    metadata = self.metadata_loader.extract_metadata(row)
                    track_id = metadata['track_id']
                    audio_path = self.file_resolver.resolve(track_id)
                    
                    if audio_path is not None:
                        tracks_with_files.append(row)
                
                attempts += batch_size
            
            if len(tracks_with_files) == 0:
                logger.error("No tracks with audio files found!")
                return
            
            logger.info(f"Found {len(tracks_with_files)} tracks with audio files (checked {attempts} candidates)")
            logger.info(f"{'DRY RUN: ' if dry_run else ''}Processing {len(tracks_with_files)} tracks...")
            
            if dry_run:
                logger.info("Dry run mode - no uploads or publishes will be performed")
                for row in tracks_with_files:
                    metadata = self.metadata_loader.extract_metadata(row)
                    track_id = metadata['track_id']
                    audio_path = self.file_resolver.resolve(track_id)
                    logger.info(f"Would process: Track {track_id} - {metadata.get('artist_name')} - {metadata.get('title')} ({metadata.get('genre_top')})")
                    logger.info(f"  File: {audio_path}")
                return
            
            # Process each track
            for row in tracks_with_files:
                try:
                    self.process_track(row)
                except Exception as e:
                    logger.error(f"Unexpected error processing track: {e}")
                    self.stats['failed'] += 1
            
            # Print summary
            self.print_summary()
            
        finally:
            # Cleanup
            self.kafka_publisher.close()
    
    def run_in_batches(self, batch_size: int = 100, dry_run: bool = False) -> None:
        """Run the producer in batch mode, processing all available tracks in batches."""
        try:
            # Load metadata
            self.metadata_loader.load()
            
            # Get all available tracks
            all_tracks = self.metadata_loader.tracks_df
            total_tracks = len(all_tracks)
            
            logger.info(f"Batch mode: Processing all {total_tracks} tracks in batches of {batch_size}")
            logger.info(f"{'DRY RUN: ' if dry_run else ''}Starting batch processing...")
            
            processed_count = 0
            batch_num = 0
            
            # Process in batches
            for start_idx in range(0, total_tracks, batch_size):
                batch_num += 1
                end_idx = min(start_idx + batch_size, total_tracks)
                batch = all_tracks.iloc[start_idx:end_idx]
                
                logger.info(f"\n{'='*60}")
                logger.info(f"BATCH {batch_num}: Processing tracks {start_idx+1}-{end_idx} of {total_tracks}")
                logger.info(f"{'='*60}")
                
                # Find tracks with files in this batch
                tracks_with_files = []
                for idx, row in batch.iterrows():
                    metadata = self.metadata_loader.extract_metadata(row)
                    track_id = metadata['track_id']
                    audio_path = self.file_resolver.resolve(track_id)
                    
                    if audio_path is not None:
                        tracks_with_files.append(row)
                
                logger.info(f"Found {len(tracks_with_files)} tracks with audio files in this batch")
                
                if len(tracks_with_files) == 0:
                    logger.warning("No tracks with audio files in this batch, skipping...")
                    continue
                
                if dry_run:
                    for row in tracks_with_files[:5]:  # Show first 5 in dry run
                        metadata = self.metadata_loader.extract_metadata(row)
                        logger.info(f"Would process: Track {metadata['track_id']} - {metadata.get('artist_name')} - {metadata.get('title')}")
                    if len(tracks_with_files) > 5:
                        logger.info(f"... and {len(tracks_with_files) - 5} more tracks")
                else:
                    # Process each track
                    for row in tracks_with_files:
                        try:
                            self.process_track(row)
                            processed_count += 1
                        except Exception as e:
                            logger.error(f"Unexpected error processing track: {e}")
                            self.stats['failed'] += 1
                
                # Print batch summary
                logger.info(f"Batch {batch_num} completed: {len(tracks_with_files)} tracks processed")
                logger.info(f"Overall progress: {processed_count}/{total_tracks} tracks")
            
            # Print final summary
            logger.info(f"\n{'='*60}")
            logger.info("FINAL SUMMARY - ALL BATCHES")
            self.print_summary()
            
        finally:
            # Cleanup
            self.kafka_publisher.close()
    
    def print_summary(self) -> None:
        """Print processing summary."""
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total tracks processed: {self.stats['total']}")
        logger.info(f"Successfully uploaded:  {self.stats['uploaded']}")
        logger.info(f"Successfully published: {self.stats['published']}")
        logger.info(f"Skipped (no file):      {self.stats['skipped']}")
        logger.info(f"Failed:                 {self.stats['failed']}")
        logger.info("=" * 60)


def main():
    """Main entry point for the FMA producer."""
    parser = argparse.ArgumentParser(
        description='FMA Audio Upload Producer - Uploads audio files to S3 and publishes events to Kafka'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        help='Number of tracks to process in single-run mode (overrides .env config)'
    )
    parser.add_argument(
        '--batch-mode',
        action='store_true',
        help='Process all tracks in batches of 100 until complete'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size when using --batch-mode (default: 100)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate the process without uploading or publishing'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = FMAProducerConfig()
    
    # Validate configuration
    errors = config.validate()
    if errors:
        logger.error("Configuration errors:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("\nPlease create a .env file based on .env.example")
        return 1
    
    logger.info("Starting FMA Audio Upload Producer")
    logger.info(f"Kafka: {config.kafka_bootstrap_servers} -> {config.kafka_topic}")
    logger.info(f"S3 Bucket: {config.s3_bucket}")
    
    # Run producer
    producer = FMAProducer(config)
    
    if args.batch_mode:
        logger.info(f"Mode: Batch processing (batch size: {args.batch_size})")
        producer.run_in_batches(batch_size=args.batch_size, dry_run=args.dry_run)
    else:
        logger.info(f"Mode: Single run (sample size: {args.sample_size or config.sample_size})")
        producer.run(sample_size=args.sample_size, dry_run=args.dry_run)
    
    logger.info("Producer completed")
    return 0


if __name__ == "__main__":
    exit(main())

