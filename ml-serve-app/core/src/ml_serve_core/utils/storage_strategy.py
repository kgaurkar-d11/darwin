import logging
import math
import re
from typing import Optional, Dict, List, Any
import requests

logger = logging.getLogger(__name__)


class ModelSizeDetector:
    """
    Detect approximate model size (bytes) using MLflow REST API.
    
    Uses direct HTTP calls instead of mlflow library to avoid heavy dependencies.
    
    Supports model URIs in formats:
    - runs:/<run_id>/<path>
    - models:/<model_name>/<version_or_stage>
    - mlflow-artifacts:/<experiment_id>/<run_id>/artifacts/<path>
    
    Args:
        tracking_uri: MLflow tracking server URI. Optional.
        username: Optional MLflow username for authentication.
        password: Optional MLflow password for authentication.
    """

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        self.tracking_uri = tracking_uri.rstrip('/') if tracking_uri else ""
        self.auth = (username, password) if username and password else None
        self.session = requests.Session() if self.tracking_uri else None
        if self.session and self.auth:
            self.session.auth = self.auth

    def get_model_size_bytes(self, model_uri: str) -> Optional[int]:
        """Get total size of model artifacts in bytes."""
        if not self.tracking_uri:
            logger.debug("ModelSizeDetector: tracking URI missing, skipping size detection")
            return None

        try:
            run_id, artifact_path = self._extract_run_and_path(model_uri)
            if not run_id:
                logger.warning(f"Could not extract run_id from model URI: {model_uri}. "
                              f"Supported formats: runs:/<run_id>/path, models:/<name>/<version>, "
                              f"or mlflow-artifacts:/<experiment_id>/<run_id>/artifacts/<path>")
                return None
            return self._sum_artifacts(run_id, artifact_path)
        except Exception as exc:
            logger.warning(f"Could not determine model size for {model_uri}: {exc}", exc_info=True)
            return None

    def _get_model_version(self, model_name: str, version: int) -> Optional[Dict[str, Any]]:
        """Get model version metadata via REST API."""
        if not self.session:
            return None

        try:
            url = f"{self.tracking_uri}/api/2.0/mlflow/model-versions/get"
            params = {"name": model_name, "version": str(version)}
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json().get("model_version")
        except Exception as e:
            logger.warning(f"Failed to get model version {model_name}/{version}: {e}")
            return None

    def _get_latest_version(self, model_name: str, stage: str) -> Optional[Dict[str, Any]]:
        """Get latest model version for stage via REST API."""
        if not self.session:
            return None

        try:
            url = f"{self.tracking_uri}/api/2.0/mlflow/model-versions/search"
            # Filter by name and stage
            filter_string = f"name='{model_name}'"
            params = {"filter": filter_string}
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            versions = response.json().get("model_versions", [])
            # Filter by stage and get latest
            stage_versions = [v for v in versions if v.get("current_stage", "").lower() == stage.lower()]
            if stage_versions:
                # Sort by version number descending
                stage_versions.sort(key=lambda v: int(v.get("version", 0)), reverse=True)
                return stage_versions[0]
            return None
        except Exception as e:
            logger.warning(f"Failed to get latest version for {model_name}/{stage}: {e}")
            return None

    def _extract_run_and_path(self, model_uri: str):
        """Extract run_id and artifact_path from model URI."""
        # Handle mlflow-artifacts:/<experiment_id>/<run_id>/artifacts/<path>
        if model_uri.startswith("mlflow-artifacts:/"):
            m = re.match(r"mlflow-artifacts:/([^/]+)/([^/]+)/artifacts/(.+)", model_uri)
            if m:
                experiment_id, run_id, artifact_path = m.groups()
                return run_id, artifact_path
            # Fallback: try without explicit artifacts/ prefix
            m = re.match(r"mlflow-artifacts:/([^/]+)/([^/]+)(/(.+))?", model_uri)
            if m:
                experiment_id, run_id, _, artifact_path = m.groups()
                return run_id, artifact_path or ""
        
        # Handle runs:/<run_id>/<path>
        if model_uri.startswith("runs:/"):
            m = re.match(r"runs:/([^/]+)(/(.+))?", model_uri)
            if not m:
                return None, None
            return m.group(1), m.group(3) or ""

        # Handle models:/<model_name>/<version_or_stage>
        if model_uri.startswith("models:/"):
            m = re.match(r"models:/([^/]+)/(.+)", model_uri)
            if not m:
                return None, None
            model_name, version_or_stage = m.group(1), m.group(2)
            
            # Get model version metadata via REST API
            if version_or_stage.isdigit():
                mv = self._get_model_version(model_name, int(version_or_stage))
            else:
                mv = self._get_latest_version(model_name, version_or_stage)
            
            if not mv:
                return None, None
            
            # Extract run_id and artifact_path from source
            # mv["source"] is typically like s3://.../<run_id>/artifacts/...
            run_id = mv.get("run_id")
            source = mv.get("source", "")
            artifact_path = source.split("/artifacts/", 1)[1] if "/artifacts/" in source else ""
            return run_id, artifact_path

        return None, None

    def _list_artifacts(self, run_id: str, path: str = "") -> List[Dict[str, Any]]:
        """List artifacts for a run via REST API."""
        if not self.session:
            return []

        try:
            url = f"{self.tracking_uri}/api/2.0/mlflow/artifacts/list"
            params = {"run_id": run_id}
            if path:
                params["path"] = path
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json().get("files", [])
        except Exception as e:
            logger.warning(f"Failed to list artifacts for run {run_id}, path {path}: {e}")
            return []

    def _sum_artifacts(self, run_id: str, path: str) -> int:
        """Calculate total size of all artifacts recursively."""
        total = 0
        stack = [path]
        
        while stack:
            current = stack.pop()
            artifacts = self._list_artifacts(run_id, current)
            
            for item in artifacts:
                if item.get("is_dir"):
                    stack.append(item["path"])
                else:
                    total += item.get("file_size", 0)
        
        return total


class StrategySelector:
    """
    Pick storage strategy based on model size thresholds.
    
    Strategies:
    - emptydir: For small models (<1GB) or unknown size. Downloads to pod-local storage.
    - pvc: For medium/large models (>=1GB). Uses shared PVC cache for node-level caching.
    
    Thresholds:
    - SMALL_GB: Models smaller than this use emptydir (default: 1.0GB)
    - LARGE_GB: Models at or above this always use pvc (default: 5.0GB)
    - Mid-range (1-5GB): Currently defaults to pvc to optimize for pod churn
    """

    SMALL_GB = 1.0  # <1GB -> emptydir
    LARGE_GB = 5.0  # >=5GB -> pvc

    def select(self, model_size_bytes: Optional[int]) -> str:
        """
        Select optimal storage strategy based on model size.
        
        Args:
            model_size_bytes: Model size in bytes, or None if unknown
            
        Returns:
            Storage strategy: 'emptydir' or 'pvc'
        """
        if not model_size_bytes:
            return "emptydir"
        size_gb = model_size_bytes / (1024 ** 3)
        if size_gb < self.SMALL_GB:
            return "emptydir"
        if size_gb >= self.LARGE_GB:
            return "pvc"
        # mid-range: choose pvc to avoid repeated downloads on churn
        return "pvc"

    def format_size(self, model_size_bytes: Optional[int]) -> str:
        """Format byte size in human-readable format."""
        if not model_size_bytes:
            return "unknown"
        size = float(model_size_bytes)
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = int(min(len(units) - 1, math.floor(math.log(size, 1024)))) if size > 0 else 0
        return f"{size / (1024 ** idx):.2f} {units[idx]}"


def determine_storage_strategy(
    user_strategy: str,
    model_uri: str,
    tracking_uri: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """
    Determine the storage strategy for a model deployment.

    Args:
        user_strategy: One of "auto", "emptydir", or "pvc".
        model_uri: MLflow model URI to evaluate.
        tracking_uri: Optional MLflow tracking URI.
        username: Optional MLflow username.
        password: Optional MLflow password.

    Returns:
        Either "emptydir" or "pvc".

    Raises:
        ValueError: If the requested strategy is not supported.
    """
    selector = StrategySelector()
    if user_strategy == "auto":
        detector = ModelSizeDetector(
            tracking_uri=tracking_uri,
            username=username,
            password=password,
        )
        model_size_bytes = detector.get_model_size_bytes(model_uri)
        storage_strategy = selector.select(model_size_bytes)
        model_size_str = selector.format_size(model_size_bytes) if model_size_bytes else "unknown"
        logger.info(f"Model size {model_size_str} recommends strategy: {storage_strategy}")
    else:
        storage_strategy = user_strategy
        logger.info(f"Using user-specified storage strategy: {storage_strategy}")

    if storage_strategy not in {"emptydir", "pvc"}:
        raise ValueError("Unsupported storage_strategy '{}'".format(storage_strategy))

    return storage_strategy
