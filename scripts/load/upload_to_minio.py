import argparse
import os
from pathlib import Path

import boto3
from botocore.client import Config
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

load_dotenv(PROJECT_ROOT / ".env")


def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError("Missing MINIO_ROOT_USER or MINIO_ROOT_PASSWORD")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket_name):
    existing_buckets = [
        bucket["Name"]
        for bucket in client.list_buckets().get("Buckets", [])
    ]

    if bucket_name not in existing_buckets:
        client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")


def upload_directory(client, local_dir: Path, bucket_name: str):
    if not local_dir.exists():
        print(f"Directory does not exist, skipping: {local_dir}")
        return

    ensure_bucket(client, bucket_name)

    files = [path for path in local_dir.rglob("*") if path.is_file()]

    for file_path in files:
        object_key = file_path.relative_to(local_dir).as_posix()

        client.upload_file(
            Filename=str(file_path),
            Bucket=bucket_name,
            Key=object_key,
        )

        print(f"Uploaded {file_path} to s3://{bucket_name}/{object_key}")

    print(f"Uploaded {len(files)} files to bucket {bucket_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--zone",
        choices=["raw", "processed", "all"],
        default="all",
    )
    args = parser.parse_args()

    client = get_minio_client()

    if args.zone in ["raw", "all"]:
        upload_directory(client, RAW_DIR, "raw")

    if args.zone in ["processed", "all"]:
        upload_directory(client, PROCESSED_DIR, "processed")


if __name__ == "__main__":
    main()