"""
Audio Processing Service for feature extraction
Extracts audio features matching the Silver notebook processing logic:
- Tempo and beat strength (2 features)
- 13 MFCC means + 13 MFCC std (26 features)
- Spectral centroid mean + std (2 features)
- 12 Chroma means + 12 Chroma std (24 features)
- Spectral rolloff mean + std (2 features)
- Spectral bandwidth mean + std (2 features)
- Spectral contrast means + std (7 bands each, 14 features)
- Zero crossing rate mean + std (2 features)
Total: 74 genre-discriminative features
"""
import librosa
import numpy as np
import soundfile as sf
from typing import Tuple, Optional
import logging
import os

logger = logging.getLogger(__name__)


class AudioProcessor:
    """Service for extracting audio features for clustering"""
    
    def __init__(self, config):
        """Initialize audio processor with configuration
        
        Args:
            config: Configuration object with audio processing settings
        """
        self.config = config
        self.sample_rate = config.AUDIO_SAMPLE_RATE
        self.duration = config.AUDIO_DURATION
        self.n_mfcc = config.N_MFCC
        
    def extract_features(self, audio_file_path: str) -> Optional[np.ndarray]:
        """Extract audio features from file (matching Silver notebook logic)
        
        Extracts 74 genre-discriminative features:
        - Tempo and beat strength (2)
        - MFCC means and std (26)
        - Spectral centroid mean and std (2)
        - Chroma means and std (24)
        - Spectral rolloff mean and std (2)
        - Spectral bandwidth mean and std (2)
        - Spectral contrast means and std (14)
        - Zero crossing rate mean and std (2)
        
        Args:
            audio_file_path: Path to audio file (mp3, wav, ogg)
            
        Returns:
            numpy array of 74 features, or None if extraction fails
        """
        try:
            logger.info(f"Loading audio file: {audio_file_path}")
            
            # Load audio file (first 30 seconds at 22050 Hz)
            # This matches the Silver notebook processing
            y, sr = librosa.load(
                audio_file_path,
                sr=self.sample_rate,
                duration=self.duration,
                mono=True
            )
            
            logger.info(f"Loaded audio: {len(y)} samples at {sr} Hz")
            
            # Tempo and rhythm features
            tempo, beats = librosa.beat.beat_track(y=y, sr=sr)
            tempo_value = float(tempo)
            beat_strength = float(np.mean(librosa.util.sync(np.vstack([librosa.feature.rms(y=y)]), beats, aggregate=np.median)))
            
            # MFCC features (timbre) - mean and variance
            mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=self.n_mfcc)
            mfcc_means = np.mean(mfccs, axis=1)
            mfcc_std = np.std(mfccs, axis=1)
            
            # Spectral centroid (brightness)
            sc = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
            sc_mean = float(np.mean(sc))
            sc_std = float(np.std(sc))
            
            # Chroma features (harmony/tonality) - critical for genre distinction
            chroma = librosa.feature.chroma_stft(y=y, sr=sr)
            chroma_means = np.mean(chroma, axis=1)
            chroma_std = np.std(chroma, axis=1)
            
            # Spectral rolloff (frequency content distribution)
            rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)[0]
            rolloff_mean = float(np.mean(rolloff))
            rolloff_std = float(np.std(rolloff))
            
            # Spectral bandwidth (timbre texture)
            bandwidth = librosa.feature.spectral_bandwidth(y=y, sr=sr)[0]
            bandwidth_mean = float(np.mean(bandwidth))
            bandwidth_std = float(np.std(bandwidth))
            
            # Spectral contrast (timbral texture, distinguishes genre well)
            contrast = librosa.feature.spectral_contrast(y=y, sr=sr, n_bands=6)
            contrast_means = np.mean(contrast, axis=1)
            contrast_std = np.std(contrast, axis=1)
            
            # Zero crossing rate (percussiveness/noisiness)
            zcr = librosa.feature.zero_crossing_rate(y)[0]
            zcr_mean = float(np.mean(zcr))
            zcr_std = float(np.std(zcr))
            
            # Assemble features in the same order as Silver notebook
            features = np.concatenate([
                [tempo_value, beat_strength],
                mfcc_means,
                mfcc_std,
                [sc_mean, sc_std],
                chroma_means,
                chroma_std,
                [rolloff_mean, rolloff_std],
                [bandwidth_mean, bandwidth_std],
                contrast_means,
                contrast_std,
                [zcr_mean, zcr_std]
            ])
            
            # Validate feature count
            if len(features) != self.config.FEATURE_COUNT:
                logger.error(
                    f"Feature count mismatch: expected {self.config.FEATURE_COUNT}, "
                    f"got {len(features)}"
                )
                return None
            
            logger.info(f"Successfully extracted {len(features)} genre-discriminative features")
            return features
            
        except Exception as e:
            logger.error(f"Feature extraction failed: {e}")
            return None
    
    def validate_audio_file(self, file_path: str) -> bool:
        """Validate that file is a readable audio file
        
        Args:
            file_path: Path to audio file
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check file exists
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                return False
            
            # Check file size (not empty)
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.error(f"File is empty: {file_path}")
                return False
            
            # Check file extension
            ext = os.path.splitext(file_path)[1].lower().lstrip('.')
            if ext not in self.config.ALLOWED_EXTENSIONS:
                logger.error(f"Invalid file extension: {ext}")
                return False
            
            # Try to load audio metadata (quick check)
            info = sf.info(file_path)
            logger.info(f"Audio file valid: {info.duration:.2f}s, {info.samplerate} Hz, {info.channels} channels")
            
            return True
            
        except Exception as e:
            logger.error(f"Audio validation failed: {e}")
            return False
    
    def process_upload(
        self,
        file_path: str,
        model_service
    ) -> Tuple[Optional[int], Optional[dict]]:
        """Process uploaded audio file end-to-end
        
        Steps:
        1. Validate audio file
        2. Extract features
        3. Normalize features
        4. Predict cluster
        
        Args:
            file_path: Path to uploaded audio file
            model_service: ModelService instance for prediction
            
        Returns:
            Tuple of (cluster_id, metadata_dict) or (None, error_dict)
        """
        try:
            # Step 1: Validate
            if not self.validate_audio_file(file_path):
                return None, {"error": "Invalid audio file"}
            
            # Step 2: Extract features
            features = self.extract_features(file_path)
            if features is None:
                return None, {"error": "Feature extraction failed"}
            
            # Step 3: Normalize features
            normalized_features = model_service.normalize_features(features)
            
            # Step 4: Predict cluster
            cluster_id = model_service.predict_cluster(normalized_features)
            
            # Return success with metadata
            metadata = {
                "cluster_id": cluster_id,
                "features": {
                    "tempo": float(features[0]),
                    "beat_strength": float(features[1]),
                    "mfcc_means": [float(x) for x in features[2:15]],
                    "mfcc_std": [float(x) for x in features[15:28]],
                    "spectral_centroid_mean": float(features[28]),
                    "spectral_centroid_std": float(features[29]),
                    "chroma_means": [float(x) for x in features[30:42]],
                    "chroma_std": [float(x) for x in features[42:54]],
                    "spectral_rolloff_mean": float(features[54]),
                    "spectral_rolloff_std": float(features[55]),
                    "spectral_bandwidth_mean": float(features[56]),
                    "spectral_bandwidth_std": float(features[57]),
                    "spectral_contrast_means": [float(x) for x in features[58:65]],
                    "spectral_contrast_std": [float(x) for x in features[65:72]],
                    "zcr_mean": float(features[72]),
                    "zcr_std": float(features[73])
                },
                "feature_count": len(features)
            }
            
            logger.info(f"Successfully processed upload: cluster {cluster_id}")
            return cluster_id, metadata
            
        except Exception as e:
            logger.error(f"Upload processing failed: {e}")
            return None, {"error": str(e)}
    
    def get_audio_info(self, file_path: str) -> Optional[dict]:
        """Get audio file metadata
        
        Args:
            file_path: Path to audio file
            
        Returns:
            Dictionary with audio metadata or None
        """
        try:
            info = sf.info(file_path)
            return {
                "duration": info.duration,
                "sample_rate": info.samplerate,
                "channels": info.channels,
                "format": info.format,
                "subtype": info.subtype
            }
        except Exception as e:
            logger.error(f"Failed to get audio info: {e}")
            return None
