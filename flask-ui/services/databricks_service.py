"""
Databricks SQL Service for querying bronze, silver, and gold tables
Handles connections and data retrieval from Databricks SQL Warehouse
"""
from databricks import sql
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class DatabricksService:
    """Service for interacting with Databricks SQL Warehouse"""
    
    def __init__(self, config):
        """Initialize Databricks service with configuration
        
        Args:
            config: Configuration object with Databricks credentials
        """
        self.config = config
        self.connection = None
        
    def connect(self):
        """Establish connection to Databricks SQL Warehouse"""
        try:
            self.connection = sql.connect(
                server_hostname=self.config.DATABRICKS_SERVER_HOSTNAME,
                http_path=self.config.DATABRICKS_HTTP_PATH,
                access_token=self.config.DATABRICKS_ACCESS_TOKEN
            )
            logger.info("Successfully connected to Databricks SQL Warehouse")
            return self.connection
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def close(self):
        """Close Databricks connection"""
        if self.connection:
            self.connection.close()
            logger.info("Closed Databricks connection")
    
    def _execute_query(self, query: str) -> List[Dict]:
        """Execute SQL query and return results as list of dictionaries
        
        Args:
            query: SQL query string
            
        Returns:
            List of dictionaries representing rows
        """
        # Ensure connection is active, reconnect if needed
        if not self.connection:
            logger.info("No connection found, reconnecting...")
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows and convert to list of dicts
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results
            
        except Exception as e:
            # If connection error, try to reconnect once
            if "closed connection" in str(e).lower() or "not connected" in str(e).lower():
                logger.warning(f"Connection lost, attempting to reconnect: {e}")
                try:
                    self.connect()
                    # Retry the query
                    cursor = self.connection.cursor()
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    cursor.close()
                    logger.info(f"Query executed successfully after reconnect, returned {len(results)} rows")
                    return results
                except Exception as retry_error:
                    logger.error(f"Query execution failed after reconnect: {retry_error}")
                    raise
            else:
                logger.error(f"Query execution failed: {e}")
                raise
    
    def get_all_tracks(self) -> List[Dict]:
        """Get all tracks with cluster assignments and metadata
        
        Returns:
            List of track dictionaries with fields:
                - track_id: int
                - cluster_id: int
                - filename: str
                - path: str (S3 path)
        """
        query = f"""
        SELECT 
            g.track_id,
            g.cluster_id,
            b.path,
            REGEXP_EXTRACT(b.path, '([^/]+)$', 1) as filename
        FROM {self.config.get_table_name('gold', 'audio_clusters')} g
        INNER JOIN {self.config.get_table_name('bronze', 'audio_raw')} b
            ON g.track_id = b.track_id
        ORDER BY g.track_id
        """
        
        return self._execute_query(query)
    
    def get_track_by_id(self, track_id: int) -> Optional[Dict]:
        """Get single track by ID
        
        Args:
            track_id: Track identifier
            
        Returns:
            Track dictionary or None if not found
        """
        query = f"""
        SELECT 
            g.track_id,
            g.cluster_id,
            b.path,
            REGEXP_EXTRACT(b.path, '([^/]+)$', 1) as filename
        FROM {self.config.get_table_name('gold', 'audio_clusters')} g
        INNER JOIN {self.config.get_table_name('bronze', 'audio_raw')} b
            ON g.track_id = b.track_id
        WHERE g.track_id = {track_id}
        """
        
        results = self._execute_query(query)
        return results[0] if results else None
    
    def get_tracks_by_cluster(self, cluster_id: int, exclude_track_id: Optional[int] = None, limit: int = 10) -> List[Dict]:
        """Get tracks in a specific cluster (for recommendations)
        
        Args:
            cluster_id: Cluster identifier
            exclude_track_id: Optional track ID to exclude from results
            limit: Maximum number of tracks to return
            
        Returns:
            List of track dictionaries in the same cluster
        """
        exclude_clause = f"AND g.track_id != {exclude_track_id}" if exclude_track_id else ""
        
        query = f"""
        SELECT 
            g.track_id,
            g.cluster_id,
            b.path,
            REGEXP_EXTRACT(b.path, '([^/]+)$', 1) as filename
        FROM {self.config.get_table_name('gold', 'audio_clusters')} g
        INNER JOIN {self.config.get_table_name('bronze', 'audio_raw')} b
            ON g.track_id = b.track_id
        WHERE g.cluster_id = {cluster_id}
            {exclude_clause}
        ORDER BY RAND()
        LIMIT {limit}
        """
        
        return self._execute_query(query)
    
    def get_cluster_stats(self) -> List[Dict]:
        """Get statistics on track distribution across clusters
        
        Returns:
            List of dictionaries with cluster_id and count
        """
        query = f"""
        SELECT 
            cluster_id,
            COUNT(*) as count
        FROM {self.config.get_table_name('gold', 'audio_clusters')}
        GROUP BY cluster_id
        ORDER BY cluster_id
        """
        
        return self._execute_query(query)
    
    def get_silver_features_sample(self, sample_size: int = 1000) -> List[Dict]:
        """Get sample of normalized features from silver layer for scaler fitting
        
        Args:
            sample_size: Number of samples to retrieve
            
        Returns:
            List of feature vectors (for computing StandardScaler statistics)
        """
        query = f"""
        SELECT scaled_features
        FROM {self.config.get_table_name('silver', 'audio_unsupervised')}
        LIMIT {sample_size}
        """
        
        return self._execute_query(query)
