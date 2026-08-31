#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Sudeesh Varier
# Date: 2025-08-01
# Description: Enhanced script to clean up buckets in StorageGrid with compliance lock checking
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import time
from optparse import OptionParser
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import json
import os
import sys
import signal
from pathlib import Path

LOG_PATH = "/var/log/storagegrid/bucketclean.log"
URL_ENDPOINT_OSL = "https://ep.s3-no.basefarm-orange.com"
URL_ENDPOINT_STH = "https://ep.s3-se.basefarm-orange.com"

# Global flag for interrupt handling
interrupt_received = False

class InterruptHandler:
    """Handle SIGINT (Ctrl+C) and SIGTERM gracefully."""

    def __init__(self):
        self.interrupted = False
        self.original_sigint = signal.getsignal(signal.SIGINT)
        self.original_sigterm = signal.getsignal(signal.SIGTERM)

    def __enter__(self):
        """Set up signal handlers."""
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original signal handlers."""
        signal.signal(signal.SIGINT, self.original_sigint)
        signal.signal(signal.SIGTERM, self.original_sigterm)
        return False

    def _handle_interrupt(self, signum, frame):
        """Handle interrupt signals gracefully."""
        global interrupt_received
        if not self.interrupted:
            self.interrupted = True
            interrupt_received = True
            signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
            print(f"\n\n{'='*80}", flush=True)
            print(f"⚠ {signal_name} received - Gracefully shutting down...", flush=True)
            print(f"Finishing current batch and saving progress...", flush=True)
            print(f"{'='*80}\n", flush=True)
            # Log to file if logger is available
            try:
                import logging
                logger = logging.getLogger(__name__)
                if logger.hasHandlers():
                    logger.warning(f"{signal_name} received - initiating graceful shutdown")
            except:
                pass
        else:
            # Second interrupt - force exit
            print(f"\n⚠ Second interrupt received - forcing immediate exit", flush=True)
            try:
                import logging
                logger = logging.getLogger(__name__)
                if logger.hasHandlers():
                    logger.error("Second interrupt - forcing immediate exit")
            except:
                pass
            sys.exit(1)

    def check(self):
        """Check if interrupt was received."""
        return self.interrupted

class ProgressIndicator:
    """Display progress updates during batch processing."""

    def __init__(self, estimated_total_objects=None, batch_size=1000, show_progress=True):
        """
        Initialize progress indicator.

        Args:
            estimated_total_objects: Estimated total number of objects (for percentage calculation)
            batch_size: Size of each batch
            show_progress: Whether to show progress updates
        """
        self.estimated_total_objects = estimated_total_objects
        self.batch_size = batch_size
        self.show_progress = show_progress
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.batches_processed = 0
        self.total_versions_processed = 0
        self.total_objects_processed = 0
        self.last_progress_line_length = 0

    def update(self, batch_num, batch_size, total_versions, total_objects):
        """
        Update progress after processing a batch.

        Args:
            batch_num: Current batch number
            batch_size: Number of versions in this batch
            total_versions: Total versions processed so far
            total_objects: Total unique objects processed so far
        """
        if not self.show_progress:
            return

        self.batches_processed = batch_num
        self.total_versions_processed = total_versions
        self.total_objects_processed = total_objects

        # Calculate statistics
        elapsed = time.time() - self.start_time
        rate = total_versions / elapsed if elapsed > 0 else 0

        # Estimate progress
        progress_str = ""
        eta_str = ""

        if self.estimated_total_objects and self.estimated_total_objects > 0:
            progress_pct = (total_objects / self.estimated_total_objects) * 100
            progress_str = f" ({progress_pct:.1f}%)"

            # Estimate time remaining
            if rate > 0:
                versions_remaining = self.estimated_total_objects - total_versions
                eta_seconds = versions_remaining / rate
                eta_str = self._format_time(eta_seconds)

        # Format elapsed time
        elapsed_str = self._format_time(elapsed)

        # Build progress message
        msg = (f"[Batch {batch_num}] "
               f"Processed: {total_objects:,} objects ({total_versions:,} versions){progress_str} | "
               f"Rate: {rate:.1f} versions/sec | "
               f"Elapsed: {elapsed_str}")

        if eta_str:
            msg += f" | ETA: {eta_str}"

        # Check if terminal supports carriage return (interactive terminal)
        # Print updates on new line every 5 batches for better visibility and compatibility
        if batch_num % 5 == 0 or batch_num == 1:
            # Clear previous line if we were using carriage return
            if self.last_progress_line_length > 0:
                print(f"\r{' ' * self.last_progress_line_length}\r", end='', flush=True)
            # Print on new line
            print(msg, flush=True)
            self.last_progress_line_length = 0
        else:
            # Try carriage return for in-between updates
            print(f"\r{msg}", end='', flush=True)
            self.last_progress_line_length = len(msg)

    def finish(self, success=True, interrupted=False):
        """
        Print final progress summary.

        Args:
            success: Whether processing completed successfully
            interrupted: Whether processing was interrupted
        """
        if not self.show_progress:
            return

        # Clear the progress line
        print(f"\r{' ' * self.last_progress_line_length}\r", end='', flush=True)

        elapsed = time.time() - self.start_time
        elapsed_str = self._format_time(elapsed)
        rate = self.total_versions_processed / elapsed if elapsed > 0 else 0

        # Print final summary
        if interrupted:
            status = "⚠ INTERRUPTED"
        elif success:
            status = "✓ COMPLETED"
        else:
            status = "⚠ COMPLETED WITH ERRORS"

        print(f"\n{status}: Processed {self.batches_processed} batches", flush=True)
        print(f"  Objects: {self.total_objects_processed:,}", flush=True)
        print(f"  Versions: {self.total_versions_processed:,}", flush=True)
        print(f"  Time: {elapsed_str}", flush=True)
        print(f"  Average rate: {rate:.1f} versions/sec\n", flush=True)

    def _format_time(self, seconds):
        """Format seconds into human-readable time string."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"

class BucketCleanupManager:
    def __init__(self, endpoint_url, profile_name=None, access_key=None, secret_key=None, debug=False, dry_run=False, force=False):
        self.endpoint_url = endpoint_url
        self.debug = debug
        self.dry_run = dry_run
        self.force = force
        
        # Initialize S3 client with credentials
        self.s3 = self._initialize_s3_client(endpoint_url, profile_name, access_key, secret_key)
        
        # Tracking dictionaries
        self.deletion_status = {}
        self.compliance_locked_objects = []
        self.failed_deletions = []
        self.stats = {
            'total_objects': 0,
            'total_versions': 0,
            'successfully_deleted_objects': 0,
            'successfully_deleted_versions': 0,
            'failed_deletions': 0,
            'compliance_locked_count': 0
        }
    
    def _initialize_s3_client(self, endpoint_url, profile_name=None, access_key=None, secret_key=None):
        """Initialize S3 client with various credential options and enhanced debugging."""
        if self.debug:
            print(f"DEBUG: Initializing S3 client...")
            print(f"DEBUG: Endpoint URL: {endpoint_url}")
            print(f"DEBUG: Profile name: {profile_name}")
            print(f"DEBUG: Access key provided: {bool(access_key)}")
            print(f"DEBUG: Secret key provided: {bool(secret_key)}")
        
        try:
            # Method 1: Explicit credentials
            if access_key and secret_key:
                if self.debug:
                    print("DEBUG: Using explicit access key and secret key")
                    print(f"DEBUG: Access Key: {access_key[:4]}...{access_key[-4:]}")
                    print(f"DEBUG: Secret Key: {secret_key[:4]}...{secret_key[-4:]}")
                
                logger.info("Using explicit access key and secret key")
                
                return boto3.client(
                    's3',
                    endpoint_url=endpoint_url,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    verify=True,
                    region_name='us-east-1'  # Some S3-compatible systems need this
                )
            
            # Method 2: Profile name
            elif profile_name:
                if self.debug:
                    print(f"DEBUG: Using AWS profile: {profile_name}")
                logger.info(f"Using AWS profile: {profile_name}")
                
                # Check if profile exists
                from boto3.session import Session
                available_profiles = Session().available_profiles
                if self.debug:
                    print(f"DEBUG: Available profiles: {available_profiles}")
                
                if profile_name not in available_profiles:
                    raise ValueError(f"Profile '{profile_name}' not found in available profiles: {available_profiles}")
                
                profile_session = boto3.Session(profile_name=profile_name)
                
                # Get credentials from session to debug
                credentials = profile_session.get_credentials()
                if credentials:
                    if self.debug:
                        print(f"DEBUG: Profile credentials loaded - Access Key: {credentials.access_key[:4]}...{credentials.access_key[-4:]}")
                else:
                    error_msg = "No credentials found in profile"
                    logger.error(error_msg)
                    if self.debug:
                        print(f"DEBUG: {error_msg}")
                    raise NoCredentialsError()
                
                return profile_session.client('s3', endpoint_url=endpoint_url, verify=True, region_name='us-east-1')
            
            # Method 3: Default credentials
            else:
                if self.debug:
                    print("DEBUG: Using default AWS credentials")
                logger.info("Using default AWS credentials")
                
                # Check default credentials
                session = boto3.Session()
                credentials = session.get_credentials()
                if credentials:
                    if self.debug:
                        print(f"DEBUG: Default credentials found - Access Key: {credentials.access_key[:4]}...{credentials.access_key[-4:]}")
                else:
                    error_msg = "No default credentials found"
                    logger.error(error_msg)
                    if self.debug:
                        print(f"DEBUG: {error_msg}")
                    raise NoCredentialsError()
                
                return boto3.client('s3', endpoint_url=endpoint_url, verify=True, region_name='us-east-1')
            
        except NoCredentialsError as e:
            error_msg = "No AWS credentials found. Please configure credentials using one of: 1) AWS credentials file (~/.aws/credentials), 2) Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY), 3) Command line options (--access-key, --secret-key)"
            logger.error(error_msg)
            if self.debug:
                print(f"DEBUG: NoCredentialsError: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            if self.debug:
                print(f"DEBUG: Exception during S3 client initialization: {e}")
                print(f"DEBUG: Exception type: {type(e)}")
            raise

    def test_credentials(self):
        """Test if credentials are working with enhanced debugging."""
        if self.debug:
            print("DEBUG: Testing credentials...")
        
        try:
            if self.debug:
                print(f"DEBUG: S3 client endpoint: {self.s3._endpoint.host}")
                print(f"DEBUG: S3 client region: {getattr(self.s3._client_config, 'region_name', 'None')}")
                print("DEBUG: Attempting to list buckets...")
            
            response = self.s3.list_buckets()
            
            if self.debug:
                print(f"DEBUG: List buckets successful - found {len(response.get('Buckets', []))} buckets")
                if response.get('Buckets'):
                    bucket_names = [bucket['Name'] for bucket in response['Buckets'][:5]]
                    print(f"DEBUG: Sample bucket names: {bucket_names}")
        
            logger.info("✓ Credentials are valid - successfully connected to S3 endpoint")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if self.debug:
                print(f"DEBUG: ClientError - Code: {error_code}")
                print(f"DEBUG: ClientError - Message: {error_message}")
            
            if error_code == 'InvalidAccessKeyId':
                logger.error("✗ Invalid access key ID")
            elif error_code == 'SignatureDoesNotMatch':
                logger.error("✗ Invalid secret access key")
            elif error_code == 'AccessDenied':
                logger.warning("⚠ Credentials valid but no bucket listing permission (this is OK)")
                return True  # Credentials work, just no list permission
            else:
                logger.error(f"✗ Credential test failed: {error_code} - {error_message}")
            return False
            
        except Exception as e:
            logger.error(f"✗ Credential test failed: {e}")
            if self.debug:
                print(f"DEBUG: Unexpected error during credential test: {e}")
                print(f"DEBUG: Error type: {type(e)}")
            return False

    def check_bucket_object_lock_configuration(self, bucket_name):
        """
        Check if the bucket has Object Lock configuration enabled.
        
        Returns:
            dict: {'enabled': bool, 'configuration': dict or None}
        """
        try:
            response = self.s3.get_object_lock_configuration(Bucket=bucket_name)
            
            if self.debug:
                print(f"DEBUG: Bucket {bucket_name} has Object Lock configuration")
                print(f"DEBUG: Object Lock config: {response.get('ObjectLockConfiguration', {})}")
            
            logger.info(f"✓ Bucket {bucket_name} has Object Lock configuration enabled")
            
            return {
                'enabled': True,
                'configuration': response.get('ObjectLockConfiguration', {})
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code in ['ObjectLockConfigurationNotFoundError', 'NoSuchObjectLockConfiguration', 'InvalidRequest']:
                if self.debug:
                    print(f"DEBUG: Bucket {bucket_name} does not have Object Lock configuration (error: {error_code})")
                
                logger.info(f"✓ Bucket {bucket_name} does not have Object Lock configuration - skipping compliance checks")
                
                return {
                    'enabled': False,
                    'configuration': None
                }
            else:
                logger.warning(f"Unexpected error (code: {error_code}) checking Object Lock configuration for bucket {bucket_name}: {e}")
                # Unknown error - log and re-raise so the caller can decide how to handle it
                raise

    def check_compliance_lock(self, bucket_name, key, version_id=None):
        """Check if an object has compliance lock and if it's still active."""
        try:
            params = {'Bucket': bucket_name, 'Key': key}
            if version_id:
                params['VersionId'] = version_id
                
            response = self.s3.get_object_retention(**params)
            
            if 'Retention' in response:
                retention_until = response['Retention']['RetainUntilDate']
                current_time = datetime.now(timezone.utc)
                
                can_delete = retention_until <= current_time
                
                return {
                    'locked': True,
                    'retention_until': retention_until,
                    'can_delete': can_delete,
                    'mode': response['Retention'].get('Mode', 'GOVERNANCE')
                }
            else:
                return {'locked': False, 'retention_until': None, 'can_delete': True}
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error'].get('Message', '')
            if error_code in ['NoSuchObjectLockConfiguration', 'InvalidRequest']:
                # These errors indicate no object lock, which is fine
                return {'locked': False, 'retention_until': None, 'can_delete': True}
            elif (error_code == 'MethodNotAllowed' or
                  (error_code == 'InvalidArgument' and 'version id' in error_message.lower())):
                # StorageGrid doesn't support per-object GetObjectRetention; use bucket default.
                return {'locked': 'use_bucket_default', 'retention_until': None, 'can_delete': None}
            else:
                logger.warning(f"Error checking compliance lock for {key}: {e}")
                # For other errors, assume locked to be safe
                return {'locked': True, 'retention_until': None, 'can_delete': False}

    def _calc_default_retention_expiry(self, last_modified, lock_configuration):
        """
        Calculate whether an object is still within the bucket's default retention period.
        Used as a fallback when GetObjectRetention fails (e.g. StorageGrid returning
        InvalidArgument for objects with no per-object retention override).

        Returns:
            tuple: (is_locked: bool, retention_until: datetime or None)
        """
        if not lock_configuration:
            return False, None

        default_ret = lock_configuration.get('Rule', {}).get('DefaultRetention', {})
        days = default_ret.get('Days')
        years = default_ret.get('Years')

        if days is None and years is None:
            return False, None

        retention_days = days if days is not None else (years * 365)
        retention_until = last_modified + timedelta(days=retention_days)
        is_locked = datetime.now(timezone.utc) < retention_until
        return is_locked, retention_until

    def get_object_count(self, bucket_name, prefix=""):
        """Get total count of objects in the bucket."""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            paginate_params = {'Bucket': bucket_name}
            if prefix:
                paginate_params['Prefix'] = prefix
            page_iterator = paginator.paginate(**paginate_params)

            total_count = 0
            for page in page_iterator:
                if 'Contents' in page:
                    total_count += len(page['Contents'])

            return total_count
        except ClientError as e:
            logger.error(f"Error counting objects in bucket {bucket_name}: {e}")
            return 0

    def get_all_object_versions(self, bucket_name, prefix=""):
        """Get all object versions in the bucket with batched processing."""
        versions_by_object = defaultdict(list)

        try:
            paginator = self.s3.get_paginator('list_object_versions')
            paginate_params = {'Bucket': bucket_name}
            if prefix:
                paginate_params['Prefix'] = prefix
            page_iterator = paginator.paginate(**paginate_params)
            
            processed_versions = 0
            for page in page_iterator:
                # Process current versions
                if 'Versions' in page:
                    for version in page['Versions']:
                        versions_by_object[version['Key']].append({
                            'VersionId': version['VersionId'],
                            'IsLatest': version['IsLatest'],
                            'LastModified': version['LastModified'],
                            'Size': version['Size'],
                            'Type': 'Version'
                        })
                        processed_versions += 1
                
                # Process delete markers
                if 'DeleteMarkers' in page:
                    for delete_marker in page['DeleteMarkers']:
                        versions_by_object[delete_marker['Key']].append({
                            'VersionId': delete_marker['VersionId'],
                            'IsLatest': delete_marker['IsLatest'],
                            'LastModified': delete_marker['LastModified'],
                            'Size': 0,
                            'Type': 'DeleteMarker'
                        })
                        processed_versions += 1
                
                # Progress update for large buckets
                if processed_versions % 1000 == 0 and self.debug:
                    print(f"DEBUG: Processed {processed_versions} versions...")
            
            return dict(versions_by_object)

        except ClientError as e:
            logger.error(f"Error retrieving object versions in bucket {bucket_name}: {e}")
            return {}

    def stream_object_versions_in_batches(self, bucket_name, prefix="", batch_size=1000):
        """
        Generator that yields batches of object versions for memory-efficient processing.
        Each batch is a dict {object_key: [versions]} with up to batch_size total versions.

        This allows processing of arbitrarily large buckets without loading all metadata into memory.
        """
        try:
            paginator = self.s3.get_paginator('list_object_versions')
            paginate_params = {'Bucket': bucket_name}
            if prefix:
                paginate_params['Prefix'] = prefix
            page_iterator = paginator.paginate(**paginate_params)

            current_batch = defaultdict(list)
            current_batch_size = 0
            total_processed = 0

            for page in page_iterator:
                # Process current versions
                if 'Versions' in page:
                    for version in page['Versions']:
                        current_batch[version['Key']].append({
                            'VersionId': version['VersionId'],
                            'IsLatest': version['IsLatest'],
                            'LastModified': version['LastModified'],
                            'Size': version['Size'],
                            'Type': 'Version'
                        })
                        current_batch_size += 1
                        total_processed += 1

                        # Yield batch when it reaches the target size
                        if current_batch_size >= batch_size:
                            if self.debug:
                                print(f"DEBUG: Yielding batch of {current_batch_size} versions (total processed: {total_processed})")
                            yield dict(current_batch)
                            current_batch = defaultdict(list)
                            current_batch_size = 0

                # Process delete markers
                if 'DeleteMarkers' in page:
                    for delete_marker in page['DeleteMarkers']:
                        current_batch[delete_marker['Key']].append({
                            'VersionId': delete_marker['VersionId'],
                            'IsLatest': delete_marker['IsLatest'],
                            'LastModified': delete_marker['LastModified'],
                            'Size': 0,
                            'Type': 'DeleteMarker'
                        })
                        current_batch_size += 1
                        total_processed += 1

                        # Yield batch when it reaches the target size
                        if current_batch_size >= batch_size:
                            if self.debug:
                                print(f"DEBUG: Yielding batch of {current_batch_size} versions (total processed: {total_processed})")
                            yield dict(current_batch)
                            current_batch = defaultdict(list)
                            current_batch_size = 0

            # Yield any remaining versions in the final partial batch
            if current_batch_size > 0:
                if self.debug:
                    print(f"DEBUG: Yielding final batch of {current_batch_size} versions (total processed: {total_processed})")
                yield dict(current_batch)

        except ClientError as e:
            logger.error(f"Error streaming object versions in bucket {bucket_name}: {e}")
            # Yield empty dict to allow graceful handling
            return

    def check_bucket_compliance_locks(self, bucket_name, versions_by_object):
        """
        Check compliance locks for all objects in the bucket.
        First checks if bucket has Object Lock enabled before checking individual objects.
        """
        logger.info("Checking bucket Object Lock configuration...")
        
        # Step 1: Check if bucket has Object Lock configuration
        try:
            object_lock_config = self.check_bucket_object_lock_configuration(bucket_name)
        except ClientError as e:
            logger.error(f"✗ Cannot determine Object Lock status for bucket {bucket_name}: {e}. Aborting to be safe.")
            return False
        
        if not object_lock_config['enabled']:
            logger.info("✓ Bucket has no Object Lock configuration - no compliance locks to check")
            if self.debug:
                print("DEBUG: Skipping individual object compliance checks - bucket has no Object Lock")
            return True  # No Object Lock means no compliance issues
        
        # Step 2: If Object Lock is enabled, check individual objects
        logger.info("Bucket has Object Lock enabled - checking compliance locks for all objects...")
        
        future_locked_objects = []
        past_locked_objects = []
        checked_count = 0
        total_to_check = sum(len(versions) for versions in versions_by_object.values())
        
        if self.debug:
            print(f"DEBUG: Checking compliance locks for {total_to_check} object versions...")
        
        default_mode = (
            object_lock_config['configuration']
            .get('Rule', {}).get('DefaultRetention', {}).get('Mode', 'UNKNOWN')
        )

        for object_key, versions in versions_by_object.items():
            for version in versions:
                compliance_info = self.check_compliance_lock(
                    bucket_name, object_key, version['VersionId']
                )

                # StorageGrid fallback: per-object retention unavailable, use bucket default
                if compliance_info['locked'] == 'use_bucket_default':
                    is_locked, retention_until = self._calc_default_retention_expiry(
                        version['LastModified'], object_lock_config['configuration']
                    )
                    compliance_info = {
                        'locked': is_locked,
                        'retention_until': retention_until,
                        'can_delete': not is_locked,
                        'mode': default_mode
                    }
                    if self.debug:
                        print(f"DEBUG: Used bucket-default retention for {object_key} "
                              f"(version {version['VersionId'][:8]}...) "
                              f"- locked: {is_locked}, expires: {retention_until}")

                checked_count += 1
                if checked_count % 100 == 0 and self.debug:
                    print(f"DEBUG: Checked compliance locks for {checked_count}/{total_to_check} versions...")
                
                if compliance_info['locked']:
                    self.stats['compliance_locked_count'] += 1
                    
                    if not compliance_info['can_delete']:
                        mode = compliance_info.get('mode', 'UNKNOWN')
                        if self.force and mode == 'GOVERNANCE':
                            logger.info(f"Force-bypassing governance lock: {object_key} (version: {version['VersionId']})")
                        else:
                            future_locked_objects.append({
                                'key': object_key,
                                'version_id': version['VersionId'],
                                'retention_until': compliance_info['retention_until'],
                                'mode': mode
                            })
                    else:
                        past_locked_objects.append({
                            'key': object_key,
                            'version_id': version['VersionId'],
                            'retention_until': compliance_info['retention_until']
                        })
        
        # Log results
        if past_locked_objects:
            logger.info(f"Found {len(past_locked_objects)} objects with expired compliance locks (can be deleted)")
            if self.debug:
                print(f"DEBUG: {len(past_locked_objects)} objects have expired compliance locks")
        
        if future_locked_objects:
            logger.error(f"Found {len(future_locked_objects)} objects with active compliance locks (CANNOT be deleted)")
            for obj in future_locked_objects:
                logger.error(f"Active lock: {obj['key']} (version: {obj['version_id']}, expires: {obj['retention_until']}, mode: {obj['mode']})")
            
            self.compliance_locked_objects = future_locked_objects
            return False
        
        logger.info(f"✓ Checked {checked_count} object versions - no blocking compliance locks found")
        return True

    def check_batch_compliance_locks(self, bucket_name, versions_batch, object_lock_config):
        """
        Check compliance locks for one batch of object versions.

        Args:
            bucket_name: Name of the S3 bucket
            versions_batch: Dict of {object_key: [versions]} for this batch
            object_lock_config: Result from check_bucket_object_lock_configuration()

        Returns:
            bool: True if batch can be deleted, False if blocking locks found
        """
        if not object_lock_config['enabled']:
            # No Object Lock means no compliance issues
            if self.debug:
                print("DEBUG: Batch has no Object Lock - skipping compliance checks")
            return True

        # Object Lock is enabled - check individual objects in this batch
        future_locked_objects = []
        past_locked_objects = []
        checked_count = 0
        total_to_check = sum(len(versions) for versions in versions_batch.values())

        if self.debug:
            print(f"DEBUG: Checking compliance locks for batch of {total_to_check} versions...")

        default_mode = (
            object_lock_config['configuration']
            .get('Rule', {}).get('DefaultRetention', {}).get('Mode', 'UNKNOWN')
        )

        for object_key, versions in versions_batch.items():
            for version in versions:
                compliance_info = self.check_compliance_lock(
                    bucket_name, object_key, version['VersionId']
                )

                # StorageGrid fallback: per-object retention unavailable, use bucket default
                if compliance_info['locked'] == 'use_bucket_default':
                    is_locked, retention_until = self._calc_default_retention_expiry(
                        version['LastModified'], object_lock_config['configuration']
                    )
                    compliance_info = {
                        'locked': is_locked,
                        'retention_until': retention_until,
                        'can_delete': not is_locked,
                        'mode': default_mode
                    }
                    if self.debug:
                        print(f"DEBUG: Used bucket-default retention for {object_key} "
                              f"(version {version['VersionId'][:8]}...) "
                              f"- locked: {is_locked}, expires: {retention_until}")

                checked_count += 1

                if compliance_info['locked']:
                    self.stats['compliance_locked_count'] += 1

                    if not compliance_info['can_delete']:
                        mode = compliance_info.get('mode', 'UNKNOWN')
                        if self.force and mode == 'GOVERNANCE':
                            logger.info(f"Force-bypassing governance lock: {object_key} (version: {version['VersionId']})")
                        else:
                            future_locked_objects.append({
                                'key': object_key,
                                'version_id': version['VersionId'],
                                'retention_until': compliance_info['retention_until'],
                                'mode': mode
                            })
                    else:
                        past_locked_objects.append({
                            'key': object_key,
                            'version_id': version['VersionId'],
                            'retention_until': compliance_info['retention_until']
                        })

        # Log results for this batch
        if past_locked_objects:
            logger.info(f"Batch: {len(past_locked_objects)} objects with expired compliance locks (can be deleted)")

        if future_locked_objects:
            logger.error(f"Batch: Found {len(future_locked_objects)} objects with active compliance locks (CANNOT be deleted)")
            for obj in future_locked_objects:
                logger.error(f"Active lock: {obj['key']} (version: {obj['version_id']}, expires: {obj['retention_until']}, mode: {obj['mode']})")

            # Accumulate blocked objects across batches
            self.compliance_locked_objects.extend(future_locked_objects)
            return False

        if self.debug:
            print(f"DEBUG: Checked {checked_count} versions in batch - no blocking locks")
        return True

    def delete_object_version(self, bucket_name, object_key, version_id, version_type):
        """Delete a specific version of an object."""
        if self.dry_run:
            bypass = " [force-bypass governance]" if self.force else ""
            logger.info(f"[DRY RUN] Would delete{bypass} {version_type}: {object_key} (version: {version_id})")
            return True
        
        try:
            params = {'Bucket': bucket_name, 'Key': object_key, 'VersionId': version_id}
            if self.force:
                params['BypassGovernanceRetention'] = True
            self.s3.delete_object(**params)
            logger.debug(f"Deleted {version_type}: {object_key} (version: {version_id})")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete {version_type}: {object_key} (version: {version_id}): {e}")
            self.failed_deletions.append({
                'key': object_key,
                'version_id': version_id,
                'type': version_type,
                'error': str(e)
            })
            return False

    def cleanup_object_versions(self, bucket_name, versions_by_object):
        """Delete all versions of all objects in the bucket using S3 batch delete API."""
        logger.info(f"Starting deletion of {len(versions_by_object)} objects...")

        all_successful = True

        # 1. Flatten all versions into a single list of tasks
        all_versions_to_delete = []
        for object_key, versions in versions_by_object.items():
            total_versions = len(versions)
            # Initialize tracking for this object
            self.deletion_status[object_key] = {
                'total_versions': total_versions,
                'deleted_versions': 0,
                'success': False
            }
            for version in versions:
                all_versions_to_delete.append({
                    'Key': object_key,
                    'VersionId': version['VersionId'],
                    'Type': version['Type']
                })

        total_versions_count = len(all_versions_to_delete)
        logger.info(f"Flattened to {total_versions_count} total object versions to delete.")

        if total_versions_count == 0:
            logger.info("No versions to delete.")
            return True

        # 2. Batch process in chunks of up to 1000 versions
        batch_size = 1000
        total_batches = (total_versions_count + batch_size - 1) // batch_size
        deleted_versions_count = 0

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_versions_count)
            current_batch = all_versions_to_delete[start_idx:end_idx]

            logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(current_batch)} items)...")
            if self.debug:
                print(f"DEBUG: Processing batch {batch_num + 1}/{total_batches} ({len(current_batch)} items)...", flush=True)

            # Dry run logic
            if self.dry_run:
                bypass = " [force-bypass governance]" if self.force else ""
                logger.info(f"[DRY RUN] Would delete batch of {len(current_batch)} object versions{bypass}")
                for item in current_batch:
                    key = item['Key']
                    self.deletion_status[key]['deleted_versions'] += 1
                    self.stats['successfully_deleted_versions'] += 1
                deleted_versions_count += len(current_batch)
                continue

            # Live execution
            try:
                # Format request for boto3
                delete_payload = {
                    'Objects': [{'Key': f['Key'], 'VersionId': f['VersionId']} for f in current_batch]
                }

                params = {
                    'Bucket': bucket_name,
                    'Delete': delete_payload
                }
                if self.force:
                    params['BypassGovernanceRetention'] = True

                response = self.s3.delete_objects(**params)

                # Process successes
                deleted_items = response.get('Deleted', [])
                for item in deleted_items:
                    key = item['Key']
                    self.deletion_status[key]['deleted_versions'] += 1
                    self.stats['successfully_deleted_versions'] += 1
                    logger.debug(f"Deleted version: {key} (version: {item.get('VersionId')})")

                deleted_versions_count += len(deleted_items)

                # Process failures
                error_items = response.get('Errors', [])
                for err in error_items:
                    key = err['Key']
                    version_id = err.get('VersionId', 'Null')
                    err_msg = f"{err.get('Code')}: {err.get('Message')}"
                    logger.error(f"Failed to delete: {key} (version: {version_id}) - {err_msg}")

                    self.stats['failed_deletions'] += 1
                    all_successful = False

                    # Find type of version for tracking
                    version_type = 'Unknown'
                    for v in current_batch:
                        if v['Key'] == key and v['VersionId'] == version_id:
                            version_type = v['Type']
                            break

                    self.failed_deletions.append({
                        'key': key,
                        'version_id': version_id,
                        'type': version_type,
                        'error': err_msg
                    })

            except ClientError as e:
                logger.error(f"Batch deletion request failed: {e}")
                all_successful = False
                for item in current_batch:
                    key = item['Key']
                    version_id = item['VersionId']
                    self.stats['failed_deletions'] += 1
                    self.failed_deletions.append({
                        'key': key,
                        'version_id': version_id,
                        'type': item['Type'],
                        'error': str(e)
                    })

            # Sub-progress status logging
            if self.debug:
                print(f"DEBUG: Batch {batch_num + 1}/{total_batches} finished. Total versions deleted: {deleted_versions_count}/{total_versions_count}", flush=True)

        # 3. Update top-level object success status based on all version results
        for object_key, status in self.deletion_status.items():
            status['success'] = (status['deleted_versions'] == status['total_versions'])
            if status['success']:
                self.stats['successfully_deleted_objects'] += 1
                logger.info(f"✓ Successfully deleted all {status['total_versions']} versions of {object_key}")
            else:
                logger.warning(f"✗ Only deleted {status['deleted_versions']}/{status['total_versions']} versions of {object_key}")

        return all_successful

    def delete_bucket(self, bucket_name):
        """Delete the bucket if it's empty."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete bucket: {bucket_name}")
            return True
        
        try:
            # Verify bucket is empty
            response = self.s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            if 'Contents' in response:
                logger.error(f"Cannot delete bucket {bucket_name}: still contains objects")
                return False
            
            # Check for versions as well
            response = self.s3.list_object_versions(Bucket=bucket_name, MaxKeys=1)
            if 'Versions' in response or 'DeleteMarkers' in response:
                logger.error(f"Cannot delete bucket {bucket_name}: still contains object versions or delete markers")
                return False
            
            # Delete the bucket
            self.s3.delete_bucket(Bucket=bucket_name)
            logger.info(f"✓ Successfully deleted bucket: {bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete bucket {bucket_name}: {e}")
            return False

    def print_summary(self, bucket_name, start_time, end_time):
        """Print comprehensive summary of the cleanup operation."""
        elapsed_time = end_time - start_time
        
        summary_output = []
        summary_output.append("\n" + "="*80)
        summary_output.append(f"BUCKET CLEANUP SUMMARY: {bucket_name}")
        summary_output.append("="*80)
        summary_output.append(f"Execution Time: {elapsed_time:.2f} seconds")
        summary_output.append(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        summary_output.append("")
        
        # Overall Statistics
        summary_output.append("OVERALL STATISTICS:")
        summary_output.append(f"  Total Objects: {self.stats['total_objects']}")
        summary_output.append(f"  Total Versions: {self.stats['total_versions']}")
        summary_output.append(f"  Successfully Deleted Objects: {self.stats['successfully_deleted_objects']}")
        summary_output.append(f"  Successfully Deleted Versions: {self.stats['successfully_deleted_versions']}")
        summary_output.append(f"  Failed Deletions: {self.stats['failed_deletions']}")
        summary_output.append(f"  Compliance Locked Items: {self.stats['compliance_locked_count']}")
        summary_output.append("")
        
        # Deletion Status Summary
        successful_objects = sum(1 for status in self.deletion_status.values() if status['success'])
        failed_objects = len(self.deletion_status) - successful_objects
        
        summary_output.append("DELETION STATUS:")
        summary_output.append(f"  Objects Fully Cleaned: {successful_objects}")
        summary_output.append(f"  Objects Partially Cleaned: {failed_objects}")
        summary_output.append("")
        
        # Failed Deletions Details
        if self.failed_deletions:
            summary_output.append("FAILED DELETIONS:")
            for failure in self.failed_deletions[:10]:  # Show first 10
                summary_output.append(f"  ✗ {failure['key']} (v:{failure['version_id'][:8]}...) - {failure['error']}")
            if len(self.failed_deletions) > 10:
                summary_output.append(f"  ... and {len(self.failed_deletions) - 10} more failures")
            summary_output.append("")
        
        # Compliance Lock Issues
        if self.compliance_locked_objects:
            summary_output.append("COMPLIANCE LOCK ISSUES:")
            for obj in self.compliance_locked_objects:
                summary_output.append(f"  🔒 {obj['key']} (expires: {obj['retention_until']})")
            summary_output.append("")
        
        # Partially Cleaned Objects
        partial_objects = [k for k, v in self.deletion_status.items() if not v['success']]
        if partial_objects:
            summary_output.append("PARTIALLY CLEANED OBJECTS:")
            for obj_key in partial_objects[:10]:  # Show first 10
                status = self.deletion_status[obj_key]
                summary_output.append(f"  ⚠ {obj_key}: {status['deleted_versions']}/{status['total_versions']} versions deleted")
            if len(partial_objects) > 10:
                summary_output.append(f"  ... and {len(partial_objects) - 10} more partial objects")
        
        summary_output.append("="*80)
        
        # Print and log the summary
        summary_text = "\n".join(summary_output)
        print(summary_text, flush=True)
        
        # Also log the summary to file
        for line in summary_output:
            logger.info(f"SUMMARY: {line}")

def debug_aws_configuration():
    """Debug AWS configuration and credentials."""
    debug_output = []
    debug_output.append("\n" + "="*60)
    debug_output.append("AWS CONFIGURATION DEBUG")
    debug_output.append("="*60)
    
    # Check environment variables
    debug_output.append("ENVIRONMENT VARIABLES:")
    aws_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION', 
                   'AWS_PROFILE', 'AWS_CONFIG_FILE', 'AWS_SHARED_CREDENTIALS_FILE']
    for var in aws_env_vars:
        value = os.environ.get(var)
        if value:
            if 'KEY' in var:
                debug_output.append(f"  {var}: {value[:4]}...{value[-4:] if len(value) > 8 else '***'}")
            else:
                debug_output.append(f"  {var}: {value}")
        else:
            debug_output.append(f"  {var}: Not set")
    
    # Check AWS files
    debug_output.append("\nAWS CONFIGURATION FILES:")
    home_dir = Path.home()
    aws_dir = home_dir / '.aws'
    
    credentials_file = aws_dir / 'credentials'
    config_file = aws_dir / 'config'
    
    debug_output.append(f"  AWS directory: {aws_dir} (exists: {aws_dir.exists()})")
    debug_output.append(f"  Credentials file: {credentials_file} (exists: {credentials_file.exists()})")
    debug_output.append(f"  Config file: {config_file} (exists: {config_file.exists()})")
    
    # Check profiles
    try:
        from boto3.session import Session
        session = Session()
        profiles = session.available_profiles
        debug_output.append(f"\nAVAILABLE PROFILES: {profiles}")
        
        # Test each profile
        for profile in profiles:
            try:
                test_session = boto3.Session(profile_name=profile)
                credentials = test_session.get_credentials()
                if credentials:
                    debug_output.append(f"  {profile}: ✓ (Access Key: {credentials.access_key[:4]}...{credentials.access_key[-4:]})")
                else:
                    debug_output.append(f"  {profile}: ✗ No credentials")
            except Exception as e:
                debug_output.append(f"  {profile}: ✗ Error: {e}")
                
    except Exception as e:
        debug_output.append(f"Error checking profiles: {e}")
    
    debug_output.append("="*60 + "\n")
    
    # Print all debug output
    debug_text = "\n".join(debug_output)
    print(debug_text, flush=True)

def main(bucket_name, endpoint_url, profile_name=None, access_key=None, secret_key=None, debug=False, dry_run=False, delete_bucket=False, force=False, prefix=""):
    """Main function to clean up the specified bucket."""
    start_time = time.time()

    # Add comprehensive debugging
    if debug:
        debug_aws_configuration()
        print(f"DEBUG: Starting main() with parameters:")
        print(f"  bucket_name: {bucket_name}")
        print(f"  prefix: {prefix or '(none)'}")
        print(f"  endpoint_url: {endpoint_url}")
        print(f"  profile_name: {profile_name}")
        print(f"  access_key provided: {bool(access_key)}")
        print(f"  secret_key provided: {bool(secret_key)}")
        print(flush=True)

    if prefix:
        logger.info(f"Starting enhanced cleanup for bucket: {bucket_name} under prefix: {prefix}")
    else:
        logger.info(f"Starting enhanced cleanup for bucket: {bucket_name}")
    
    # Initialize cleanup manager
    try:
        cleanup_manager = BucketCleanupManager(endpoint_url, profile_name, access_key, secret_key, debug, dry_run, force)
        if debug:
            print("DEBUG: BucketCleanupManager initialized successfully", flush=True)
    except Exception as e:
        error_msg = f"Failed to initialize BucketCleanupManager: {e}"
        logger.error(error_msg)
        if debug:
            print(f"DEBUG: {error_msg}", flush=True)
        return False
    
    # Test credentials first
    if debug:
        print("DEBUG: Testing credentials...", flush=True)
    if not cleanup_manager.test_credentials():
        error_msg = "Credential validation failed - aborting"
        logger.error(f"✗ {error_msg}")
        if debug:
            print(f"DEBUG: {error_msg}", flush=True)
        return False
    
    if debug:
        print("DEBUG: Credentials validated successfully", flush=True)
    
    try:
        # Step 1: Check if bucket exists
        cleanup_manager.s3.head_bucket(Bucket=bucket_name)
        logger.info(f"✓ Bucket {bucket_name} exists and is accessible")
        
    except ClientError as e:
        logger.error(f"✗ Bucket {bucket_name} does not exist or is not accessible: {e}")
        return False
    
    # Step 2: Get object count
    if debug:
        print("DEBUG: Counting objects in bucket...", flush=True)
    object_count = cleanup_manager.get_object_count(bucket_name, prefix)
    cleanup_manager.stats['total_objects'] = object_count
    if prefix:
        logger.info(f"✓ Found {object_count} objects in bucket under prefix: {prefix}")
    else:
        logger.info(f"✓ Found {object_count} objects in bucket")
    
    if object_count == 0:
        logger.info("Bucket is already empty")
        if not dry_run and delete_bucket:
            success = cleanup_manager.delete_bucket(bucket_name)
            if success:
                logger.info("✓ Empty bucket deleted successfully")
            else:
                logger.error("✗ Failed to delete empty bucket")
        elif not delete_bucket:
            logger.info("--delete-bucket not specified, skipping bucket deletion")
        end_time = time.time()
        if debug:
            cleanup_manager.print_summary(bucket_name, start_time, end_time)
        return True
    
    # Step 3: Check bucket-level Object Lock configuration (once, before streaming)
    if debug:
        print("DEBUG: Checking bucket Object Lock configuration...", flush=True)
    logger.info("Checking bucket Object Lock configuration...")

    try:
        object_lock_config = cleanup_manager.check_bucket_object_lock_configuration(bucket_name)
    except ClientError as e:
        logger.error(f"✗ Cannot determine Object Lock status for bucket {bucket_name}: {e}. Aborting to be safe.")
        end_time = time.time()
        if debug:
            cleanup_manager.print_summary(bucket_name, start_time, end_time)
        return False

    if object_lock_config['enabled']:
        logger.info("Bucket has Object Lock enabled - will check compliance locks per batch")
    else:
        logger.info("✓ Bucket has no Object Lock configuration - no compliance locks to check")

    # Step 4: Stream, check, and delete in batches (memory-efficient)
    if not dry_run:
        logger.info("Starting streaming batch processing with deletion...")
    else:
        logger.info("DRY RUN: Starting streaming batch processing (simulation)...")

    if debug:
        print("DEBUG: Processing bucket in streaming batches of 1000 versions...", flush=True)

    batch_num = 0
    all_deleted = True
    total_versions_processed = 0
    interrupted = False

    # Initialize progress indicator (always show unless explicitly disabled)
    progress = ProgressIndicator(
        estimated_total_objects=object_count,
        batch_size=1000,
        show_progress=True  # Always show progress updates
    )

    # Set up interrupt handler
    with InterruptHandler() as interrupt_handler:
        for batch in cleanup_manager.stream_object_versions_in_batches(bucket_name, prefix, batch_size=1000):
            # Check for interrupt before processing next batch
            if interrupt_handler.check():
                interrupted = True
                logger.warning(f"⚠ Interrupt received after processing {batch_num} batches")
                progress.finish(success=False, interrupted=True)
                break

            batch_num += 1
            batch_size = sum(len(versions) for versions in batch.values())
            total_versions_processed += batch_size

            if debug:
                print(f"DEBUG: Processing batch {batch_num} ({batch_size} versions)...", flush=True)
            logger.info(f"Processing batch {batch_num} ({batch_size} versions, cumulative: {total_versions_processed})...")

            # Check compliance locks for this batch
            can_proceed = cleanup_manager.check_batch_compliance_locks(bucket_name, batch, object_lock_config)

            if not can_proceed:
                logger.error(f"✗ ABORTING at batch {batch_num}: Found objects with active compliance locks that prevent deletion")
                all_deleted = False
                progress.finish(success=False, interrupted=False)
                break

            # Delete this batch
            batch_deleted = cleanup_manager.cleanup_object_versions(bucket_name, batch)
            if not batch_deleted:
                all_deleted = False
                logger.warning(f"⚠ Batch {batch_num} had deletion failures")

            # Update progress indicator
            progress.update(
                batch_num=batch_num,
                batch_size=batch_size,
                total_versions=total_versions_processed,
                total_objects=len(cleanup_manager.deletion_status)
            )

    # Finish progress display
    if not interrupted and all_deleted:
        progress.finish(success=True, interrupted=False)
    elif not interrupted:
        progress.finish(success=False, interrupted=False)

    # Update final stats
    cleanup_manager.stats['total_versions'] = total_versions_processed
    cleanup_manager.stats['total_objects'] = len(cleanup_manager.deletion_status)

    if interrupted:
        logger.warning(f"⚠ INTERRUPTED: Processed {batch_num} batches ({total_versions_processed} versions) before interrupt")
        print(f"\n{'='*80}", flush=True)
        print(f"⚠ PARTIAL COMPLETION - Interrupted by user", flush=True)
        print(f"{'='*80}", flush=True)
        print(f"Batches processed: {batch_num}", flush=True)
        print(f"Versions processed: {total_versions_processed}", flush=True)
        print(f"Objects processed: {len(cleanup_manager.deletion_status)}", flush=True)
        print(f"{'='*80}\n", flush=True)
    elif all_deleted:
        logger.info(f"✓ Successfully processed {batch_num} batches ({total_versions_processed} total versions)")
    else:
        logger.warning(f"⚠ Processed {batch_num} batches with some failures ({total_versions_processed} total versions)")

    # Step 6: Delete bucket if all objects were successfully removed
    if not interrupted and all_deleted and not dry_run and delete_bucket:
        logger.info("All objects deleted successfully, attempting to delete bucket...")
        bucket_deleted = cleanup_manager.delete_bucket(bucket_name)
    else:
        bucket_deleted = False
        if interrupted:
            logger.info("Bucket deletion skipped due to interrupt")
        elif not all_deleted:
            logger.warning("Not all objects were deleted successfully, skipping bucket deletion")
        elif dry_run:
            logger.info("DRY RUN: Would attempt to delete bucket")
        elif not delete_bucket:
            logger.info("--delete-bucket not specified, skipping bucket deletion")

    end_time = time.time()

    # Step 7: Print summary
    if debug or interrupted:
        cleanup_manager.print_summary(bucket_name, start_time, end_time)

    # Final status
    if interrupted:
        logger.warning("⚠ Bucket cleanup interrupted by user - partial progress saved")
        return False
    elif all_deleted and (bucket_deleted or dry_run or not delete_bucket):
        logger.info("✓ Bucket cleanup completed successfully")
        return True
    else:
        logger.warning("⚠ Bucket cleanup completed with issues - check logs for details")
        return False

if __name__ == "__main__":
    # Ensure log directory exists
    log_dir = os.path.dirname(LOG_PATH)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Setup logging
    logging.basicConfig(
        filename=LOG_PATH, 
        level=logging.INFO, 
        datefmt='%Y-%m-%d %H:%M:%S', 
        format="[%(asctime)s] %(filename)s:%(lineno)d (%(levelname)s): %(message)s"
    )
    
    # Reduce boto3 logging verbosity
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    
    # Command line argument parsing
    usage = "usage: %prog [options] -b BUCKET_NAME"
    parser = OptionParser(usage)
    parser.add_option("-d", "--dryrun", dest="dryrun", default=False, action="store_true", 
                      help="Dry run mode - no actual deletions will be performed")
    parser.add_option("-b", "--bucket", dest="bucket", action="store", 
                      help="Bucket name to clean (required)")
    parser.add_option("-v", "--debug", dest="debug", default=False, action="store_true", 
                      help="Enable debug mode with detailed summary")
    parser.add_option("-e", "--endpoint", dest="endpoint", default=URL_ENDPOINT_OSL, 
                      help=f"S3 endpoint URL (default: {URL_ENDPOINT_OSL})")
    
    # Credential options
    parser.add_option("-p", "--profile", dest="profile", action="store", 
                      help="AWS profile name to use (from ~/.aws/credentials)")
    parser.add_option("--access-key", dest="access_key", action="store", 
                      help="AWS access key ID")
    parser.add_option("--secret-key", dest="secret_key", action="store", 
                      help="AWS secret access key")
    parser.add_option("--delete-bucket", dest="delete_bucket", default=False, action="store_true",
                      help="Delete the bucket itself after all objects have been cleaned up")
    parser.add_option("--force", dest="force", default=False, action="store_true",
                      help="Bypass Governance mode retention locks. Compliance mode locks still block deletion.")
    parser.add_option("-x", "--prefix", dest="prefix", action="store", default="",
                      help="Only process objects under this key prefix (e.g. 'backups/node1/')")

    (options, args) = parser.parse_args()
    
    if not options.bucket:
        parser.error("Bucket name is required. Use -b or --bucket option.")
    
    # Validate credential options
    if options.access_key and not options.secret_key:
        parser.error("Both --access-key and --secret-key must be provided together.")
    if options.secret_key and not options.access_key:
        parser.error("Both --access-key and --secret-key must be provided together.")
    
    logger.info(" -------------------------  SCRIPT START -------------------------")
    logger.info(f"Target bucket: {options.bucket}")
    logger.info(f"Endpoint: {options.endpoint}")
    logger.info(f"Dry run: {options.dryrun}")
    logger.info(f"Debug mode: {options.debug}")
    logger.info(f"Profile: {options.profile or 'default'}")
    logger.info(f"Using explicit credentials: {bool(options.access_key)}")
    logger.info(f"Delete bucket after cleanup: {options.delete_bucket}")
    logger.info(f"Force governance bypass: {options.force}")
    logger.info(f"Prefix filter: {options.prefix or '(entire bucket)'}")
    
    try:
        success = main(
            options.bucket,
            options.endpoint,
            options.profile,
            options.access_key,
            options.secret_key,
            options.debug,
            options.dryrun,
            options.delete_bucket,
            options.force,
            options.prefix
        )
        exit_code = 0 if success else 1
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        print("\nScript interrupted by user", flush=True)
        exit_code = 130
    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        print(f"Unexpected error: {e}", flush=True)
        exit_code = 2
        
    print(f"\nTo know more about the details of this run, check the log file at {LOG_PATH}", flush=True)
    logger.info(" --------------------------  SCRIPT END --------------------------")
    sys.exit(exit_code)
