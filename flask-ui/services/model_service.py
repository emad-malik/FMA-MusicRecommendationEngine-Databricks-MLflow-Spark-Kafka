"""
Model Service for KMeans clustering predictions
Supports both Databricks Model Serving (REST API) and local MLflow model loading
"""
import numpy as np
import requests
from sklearn.preprocessing import StandardScaler
from typing import List, Optional, Dict
import logging
import json

logger = logging.getLogger(__name__)


class ModelService:
    """Service for making predictions using KMeans clustering model"""
    
    def __init__(self, config):
        """Initialize model service with configuration
        
        Args:
            config: Configuration object with Databricks and MLflow settings
        """
        self.config = config
        self.model = None
        self.scaler = None
        self.use_model_serving = config.USE_MODEL_SERVING
        self.serving_endpoint_url = None
        
        if self.use_model_serving and config.MODEL_SERVING_ENDPOINT:
            # Build Model Serving endpoint URL
            self.serving_endpoint_url = (
                f"https://{config.DATABRICKS_SERVER_HOSTNAME}/serving-endpoints/"
                f"{config.MODEL_SERVING_ENDPOINT}/invocations"
            )
            logger.info(f"Using Databricks Model Serving: {self.serving_endpoint_url}")
        
    def load_model(self):
        """Load or initialize model service
        
        For Model Serving: Just validates endpoint configuration
        For Local MLflow: Loads the model from MLflow
        """
        if self.use_model_serving:
            if not self.serving_endpoint_url:
                logger.error("Model Serving enabled but no endpoint configured")
                raise ValueError("MODEL_SERVING_ENDPOINT not configured")
            
            logger.info("Model Serving configured - no local model loading needed")
            # Test the endpoint
            try:
                self._test_serving_endpoint()
                logger.info("Model Serving endpoint is accessible")
            except Exception as e:
                logger.warning(f"Could not test Model Serving endpoint: {e}")
            
            return True
        else:
            # Fallback to local MLflow loading (original implementation)
            logger.info("Using local MLflow model loading")
            return self._load_local_model()
    
    def _test_serving_endpoint(self):
        """Test if the Model Serving endpoint is accessible"""
        headers = {
            'Authorization': f'Bearer {self.config.DATABRICKS_ACCESS_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        # Send a test request with dummy data
        test_data = {
            "dataframe_records": [
                {"features": [0.0] * self.config.FEATURE_COUNT}
            ]
        }
        
        response = requests.post(
            self.serving_endpoint_url,
            headers=headers,
            json=test_data,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info("Model Serving endpoint test successful")
            return True
        else:
            logger.warning(f"Model Serving endpoint test returned status {response.status_code}")
            return False
    
    def _load_local_model(self):
        """Load model from MLflow (fallback method)"""
        try:
            import mlflow
            import mlflow.sklearn
            import os
            
            # Set MLflow tracking URI
            mlflow.set_tracking_uri(self.config.MLFLOW_TRACKING_URI)
            
            # Set Databricks credentials if using Databricks MLflow
            if self.config.MLFLOW_TRACKING_URI == 'databricks':
                os.environ['DATABRICKS_HOST'] = f"https://{self.config.DATABRICKS_SERVER_HOSTNAME}"
                os.environ['DATABRICKS_TOKEN'] = self.config.DATABRICKS_ACCESS_TOKEN
            
            # Load the model
            model_uri = self.config.MLFLOW_MODEL_URI
            logger.info(f"Loading model from: {model_uri}")
            
            # Try to load as PyFunc (works for most MLflow models)
            try:
                self.model = mlflow.pyfunc.load_model(model_uri)
                logger.info("Successfully loaded model as PyFunc")
            except Exception as e:
                logger.warning(f"Failed to load as PyFunc: {e}")
                # Try loading as sklearn model directly
                self.model = mlflow.sklearn.load_model(model_uri)
                logger.info("Successfully loaded model as sklearn")
            
            return self.model
            
        except Exception as e:
            logger.error(f"Failed to load model from MLflow: {e}")
            logger.info("Will use Model Serving or compute scaler from Silver data")
            return None
    
    def fit_scaler(self, features_matrix: np.ndarray):
        """Fit StandardScaler on feature matrix
        
        Args:
            features_matrix: numpy array of shape (n_samples, n_features)
        """
        self.scaler = StandardScaler()
        self.scaler.fit(features_matrix)
        logger.info(f"Fitted StandardScaler on {features_matrix.shape[0]} samples")
        logger.info(f"Feature means: {self.scaler.mean_}")
        logger.info(f"Feature stds: {self.scaler.scale_}")
    
    def compute_scaler_from_silver(self, databricks_service):
        """Compute scaler statistics from Silver layer data
        
        Args:
            databricks_service: DatabricksService instance for querying
        """
        try:
            logger.info("Computing StandardScaler from Silver layer...")
            
            # Get sample features from Silver table
            results = databricks_service.get_silver_features_sample(sample_size=1000)
            
            if not results:
                logger.warning("No features found in Silver layer")
                return False
            
            # Extract feature vectors (they are stored as Spark Vector type)
            # We need to convert them to numpy arrays
            features_list = []
            for row in results:
                # scaled_features is a Spark ML Vector - convert to list
                features = row['scaled_features']
                if hasattr(features, 'toArray'):
                    features = features.toArray()
                elif isinstance(features, str):
                    # If it's a string representation, parse it
                    import json
                    features = json.loads(features)
                features_list.append(features)
            
            # Convert to numpy array
            features_matrix = np.array(features_list)
            
            # Fit the scaler
            self.fit_scaler(features_matrix)
            return True
            
        except Exception as e:
            logger.error(f"Failed to compute scaler from Silver layer: {e}")
            return False
    
    def normalize_features(self, features: np.ndarray) -> np.ndarray:
        """Normalize features using StandardScaler
        
        Args:
            features: Raw feature vector (74 dimensions)
            
        Returns:
            Normalized feature vector
        """
        if self.scaler is None:
            logger.warning("No scaler available - using raw features (may affect accuracy)")
            return features
        
        # Ensure features is 2D for sklearn
        if len(features.shape) == 1:
            features = features.reshape(1, -1)
        
        return self.scaler.transform(features)
    
    def predict_cluster(self, features: np.ndarray) -> int:
        """Predict cluster ID for given features
        
        Args:
            features: Normalized feature vector (74 dimensions)
            
        Returns:
            Cluster ID (integer)
        """
        # Ensure features is 2D for prediction
        if len(features.shape) == 1:
            features = features.reshape(1, -1)
        
        # Validate feature count
        if features.shape[1] != self.config.FEATURE_COUNT:
            raise ValueError(
                f"Expected {self.config.FEATURE_COUNT} features, got {features.shape[1]}"
            )
        
        if self.use_model_serving and self.serving_endpoint_url:
            return self._predict_via_serving(features)
        else:
            return self._predict_local(features)
    
    def _predict_via_serving(self, features: np.ndarray) -> int:
        """Predict cluster using Databricks Model Serving endpoint
        
        Args:
            features: Feature array (2D)
            
        Returns:
            Cluster ID
        """
        try:
            headers = {
                'Authorization': f'Bearer {self.config.DATABRICKS_ACCESS_TOKEN}',
                'Content-Type': 'application/json'
            }
            
            # Convert features to list for JSON serialization
            features_list = features.tolist()[0] if features.shape[0] == 1 else features.tolist()
            
            # Databricks Model Serving expects dataframe_records or dataframe_split format
            # For KMeans, we need to send the features in the expected format
            payload = {
                "dataframe_records": [
                    {"features": features_list}
                ]
            }
            
            logger.info(f"Sending prediction request to Model Serving endpoint")
            
            response = requests.post(
                self.serving_endpoint_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Model Serving response: {result}")
                
                # Extract prediction from response
                # Response format varies, could be {"predictions": [0]} or {"prediction": [0]}
                if 'predictions' in result:
                    cluster_id = int(result['predictions'][0])
                elif 'prediction' in result:
                    cluster_id = int(result['prediction'][0] if isinstance(result['prediction'], list) else result['prediction'])
                else:
                    # Try to get first value from response
                    cluster_id = int(list(result.values())[0][0])
                
                logger.info(f"Predicted cluster: {cluster_id}")
                return cluster_id
            else:
                logger.error(f"Model Serving request failed: {response.status_code} - {response.text}")
                raise ValueError(f"Model Serving returned status {response.status_code}")
                
        except Exception as e:
            logger.error(f"Prediction via Model Serving failed: {e}")
            raise
    
    def _predict_local(self, features: np.ndarray) -> int:
        """Predict cluster using local model
        
        Args:
            features: Feature array (2D)
            
        Returns:
            Cluster ID
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load_model() first.")
        
        try:
            # Try different prediction methods based on model type
            if hasattr(self.model, 'predict'):
                # PyFunc or sklearn model
                prediction = self.model.predict(features)
            elif hasattr(self.model, '_model_impl'):
                # MLflow PyFunc wrapper
                prediction = self.model._model_impl.predict(features)
            else:
                raise AttributeError("Model does not have a predict method")
            
            # Extract cluster ID
            cluster_id = int(prediction[0]) if isinstance(prediction, (list, np.ndarray)) else int(prediction)
            
            logger.info(f"Predicted cluster: {cluster_id}")
            return cluster_id
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            raise
    
    def get_cluster_count(self) -> int:
        """Get number of clusters in the model
        
        Returns:
            Number of clusters (typically 2 based on training notebook)
        """
        if self.model is None:
            return 2  # Default from training notebook
        
        try:
            if hasattr(self.model, 'n_clusters'):
                return self.model.n_clusters
            elif hasattr(self.model, '_model_impl') and hasattr(self.model._model_impl, 'n_clusters'):
                return self.model._model_impl.n_clusters
            else:
                return 2  # Default
        except:
            return 2
