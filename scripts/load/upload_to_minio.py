from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"Created MinIO bucket: {bucket}")


def upload_directory(client, local_dir: Path, bucket: str, prefix: str = "") -> int:
    ensure_bucket(client, bucket)
    count = 0
    if not local_dir.exists():
        print(f"No local directory to upload: {local_dir}")
        return count
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        relative_key = path.relative_to(local_dir).as_posix()
        object_key = f"{prefix.rstrip('/')}/{relative_key}" if prefix else relative_key
        client.upload_file(str(path), bucket, object_key)
        print(f"Uploaded {path} -> s3://{bucket}/{object_key}")
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload raw and/or processed data folders to MinIO.")
    parser.add_argument("--zone", choices=["raw", "processed", "all"], default="all")
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    raw_bucket = os.getenv("MINIO_BUCKET_RAW", "raw")
    processed_bucket = os.getenv("MINIO_BUCKET_PROCESSED", "processed")
    client = minio_client()

    uploaded = 0
    if args.zone in {"raw", "all"}:
        uploaded += upload_directory(client, RAW_DIR, raw_bucket)
    if args.zone in {"processed", "all"}:
        uploaded += upload_directory(client, PROCESSED_DIR, processed_bucket)
    print(f"MinIO upload finished: {uploaded} files")


if __name__ == "__main__":
    main()
