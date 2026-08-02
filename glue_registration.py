"""Airflow task: register curated vehicle-health tables in AWS Glue after Spark completes."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

from pipeline_settings import AWS_REGION, CURATED_S3_ROOT, GLUE_DATABASE


def _ensure_jobs_on_path() -> None:
    includes_dir = Path(__file__).resolve().parent / "includes"
    if not includes_dir.is_dir():
        raise RuntimeError(
            f"Missing Airflow includes directory: {includes_dir}. "
            "Upload glue_aws_client.py and glue_catalog_registry.py to dags/includes/."
        )
    path = str(includes_dir)
    if path not in sys.path:
        sys.path.insert(0, path)


def register_vehicle_health_glue_metadata(**context: Any) -> None:
    """Register Glue tables/partitions for the curated S3 datasets."""
    _ensure_jobs_on_path()
    from glue_catalog_registry import register_curated_tables_standalone

    params: Dict[str, Any] = context["params"]
    register_curated_tables_standalone(
        curated_s3_root=params.get("curated_s3_root", CURATED_S3_ROOT),
        database=params.get("glue_database", GLUE_DATABASE),
        region=params.get("aws_region", AWS_REGION),
    )
