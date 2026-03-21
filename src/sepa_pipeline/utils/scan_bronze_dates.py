import io
import re
import zipfile

from pyarrow import fs

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)


def scan_bronze_dates():
    """
    Scans all master ZIP files in the MinIO bronze layer, checking if
    the internal 'comercio.csv' footer date matches the filename date.
    """
    logger.info("Starting Historical Bronze Date Scanner...")
    config = SEPAConfig()

    s3 = fs.S3FileSystem(
        endpoint_override=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        scheme="http",
        region="us-east-1",
    )

    base_path = f"{config.minio_bucket}/bronze/raw"
    logger.info(f"Scanning base path: s3://{base_path}")

    # Use a file selector to recursively find all zip files
    selector = fs.FileSelector(base_path, recursive=True)
    try:
        files = s3.get_file_info(selector)
    except Exception as e:
        logger.error(f"Failed to access Bronze Layer: {e}")
        return

    master_zips = [
        f for f in files if f.type == fs.FileType.File and f.base_name.endswith(".zip")
    ]
    logger.info(f"Found {len(master_zips)} Master ZIP files to audit.")

    discrepancies = []
    successes = 0

    for file_info in master_zips:
        s3_path = file_info.path
        filename = file_info.base_name

        # Extract date from filename (e.g. sepa_precios_2026-02-15.zip)
        filename_date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        if not filename_date_match:
            logger.warning(f"Could not extract date from filename: {filename}")
            continue

        filename_date = filename_date_match.group(1)
        logger.info(f"Auditing {filename} (Expected date: {filename_date})")

        try:
            # Download file fully into memory. If files are huge, this could OOM,
            # but SEPA Master ZIPs are ~320MB, so it's acceptable for a one-off utility.
            with s3.open_input_stream(s3_path) as stream:
                master_bytes = stream.read()

            with zipfile.ZipFile(io.BytesIO(master_bytes), "r") as master_zf:
                inner_zips = [f for f in master_zf.namelist() if f.endswith(".zip")]

                if not inner_zips:
                    logger.warning(f"No nested '.zip' files found inside {filename}")
                    continue

                sample_size = min(20, len(inner_zips))
                sampled_zips = inner_zips[:sample_size]

                valid_count = 0
                stale_count = 0
                unknown_count = 0

                from datetime import datetime, timedelta

                target_d = datetime.strptime(filename_date, "%Y-%m-%d").date()

                for inner_zip_name in sampled_zips:
                    try:
                        with master_zf.open(inner_zip_name, "r") as inner_file:
                            with zipfile.ZipFile(
                                io.BytesIO(inner_file.read())
                            ) as inner_zf:
                                comercio_files = [
                                    f
                                    for f in inner_zf.namelist()
                                    if f.endswith("comercio.csv")
                                ]
                                if not comercio_files:
                                    unknown_count += 1
                                    continue

                                comercio_filename = comercio_files[0]
                                date_found = False

                                with inner_zf.open(comercio_filename, "r") as f:
                                    wrapper = io.TextIOWrapper(
                                        f, encoding="utf-8-sig", errors="replace"
                                    )
                                    for line in wrapper:
                                        if "ltima actualizaci" in line.lower():
                                            date_match = re.search(
                                                r"(\d{4}-\d{2}-\d{2})", line
                                            )
                                            if date_match:
                                                extracted_date_str = date_match.group(1)
                                                extracted_d = datetime.strptime(
                                                    extracted_date_str, "%Y-%m-%d"
                                                ).date()

                                                date_found = True
                                                if extracted_d < (
                                                    target_d - timedelta(days=1)
                                                ):
                                                    stale_count += 1
                                                else:
                                                    valid_count += 1
                                                break

                                if not date_found:
                                    unknown_count += 1
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse nested ZIP {inner_zip_name} "
                            f"in {filename}: {e}"
                        )
                        unknown_count += 1

                # Evaluate Consensus
                if stale_count > valid_count:
                    logger.error(
                        f"DISCREPANCY DETECTED: {filename} filename says "
                        f"{filename_date} but majority data is stale "
                        f"(Valid: {valid_count}, Stale: {stale_count}, "
                        f"Unknown: {unknown_count})"
                    )
                    discrepancies.append(
                        {
                            "file": s3_path,
                            "filename_date": filename_date,
                            "consensus": "STALE",
                            "stats": f"Valid: {valid_count}, Stale: {stale_count}",
                        }
                    )
                else:
                    logger.info(
                        f"Clean match: {filename_date} has predominantly fresh data "
                        f"(Valid: {valid_count}, Stale: {stale_count}, "
                        f"Unknown: {unknown_count})"
                    )
                    successes += 1

        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")

    logger.info("=== AUDIT SUMMARY ===")
    logger.info(f"Total files audited   : {len(master_zips)}")
    logger.info(f"Matching dates        : {successes}")
    logger.info(f"Discrepancies found   : {len(discrepancies)}")

    if discrepancies:
        logger.info("\n--- LIST OF DISCREPANCIES ---")
        for d in discrepancies:
            logger.info(
                f"Path: {d['file']} | Filename Date: {d['filename_date']} | "
                f"Consensus: {d['consensus']} ({d['stats']})"
            )


if __name__ == "__main__":
    scan_bronze_dates()
