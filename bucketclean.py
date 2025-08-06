#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Sudeesh Varier
# Date: 2025-08-01
# Description: Enhanced script to clean up buckets in S3 compliant Storage systems along with compliance lock checking
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import time
from optparse import OptionParser
from datetime import datetime, timezone
from collections import defaultdict
import json
import os
import sys
from pathlib import Path

LOG_PATH = "/var/log/bucketclean.log"
S3_URL_ENDPOINT = "supply_your_own_endpoint_here" # This would be your default endpoint if not explicitly supplied as arguments or choose to pass it as an argument

class BucketCleanupManager:
    def __init__(self, endpoint_url, profile_name=None, access_key=None, secret_key=None, debug=False, dry_run=False):
        self.endpoint_url = endpoint_url
        self.debug = debug
        self.dry_run = dry_run
        
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
            
            if error_code in ['ObjectLockConfigurationNotFoundError', 'InvalidRequest']:
                if self.debug:
                    print(f"DEBUG: Bucket {bucket_name} does not have Object Lock configuration")
                
                logger.info(f"✓ Bucket {bucket_name} does not have Object Lock configuration - skipping compliance checks")
                
                return {
                    'enabled': False,
                    'configuration': None
                }
            else:
                logger.warning(f"Error checking Object Lock configuration for bucket {bucket_name}: {e}")
                # If we can't determine the status, assume it might be enabled to be safe
                return {
                    'enabled': True,  # Assume enabled to be safe
                    'configuration': None
                }

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
            if error_code in ['NoSuchObjectLockConfiguration', 'InvalidRequest']:
                # These errors indicate no object lock, which is fine
                return {'locked': False, 'retention_until': None, 'can_delete': True}
            else:
                logger.warning(f"Error checking compliance lock for {key}: {e}")
                # For other errors, assume locked to be safe
                return {'locked': True, 'retention_until': None, 'can_delete': False}

    def get_object_count(self, bucket_name):
        """Get total count of objects in the bucket."""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name)
            
            total_count = 0
            for page in page_iterator:
                if 'Contents' in page:
                    total_count += len(page['Contents'])
            
            return total_count
        except ClientError as e:
            logger.error(f"Error counting objects in bucket {bucket_name}: {e}")
            return 0

    def get_all_object_versions(self, bucket_name):
        """Get all object versions in the bucket with batched processing."""
        versions_by_object = defaultdict(list)
        
        try:
            paginator = self.s3.get_paginator('list_object_versions')
            page_iterator = paginator.paginate(Bucket=bucket_name)
            
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

    def check_bucket_compliance_locks(self, bucket_name, versions_by_object):
        """
        Check compliance locks for all objects in the bucket.
        First checks if bucket has Object Lock enabled before checking individual objects.
        """
        logger.info("Checking bucket Object Lock configuration...")
        
        # Step 1: Check if bucket has Object Lock configuration
        object_lock_config = self.check_bucket_object_lock_configuration(bucket_name)
        
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
        
        for object_key, versions in versions_by_object.items():
            for version in versions:
                compliance_info = self.check_compliance_lock(
                    bucket_name, object_key, version['VersionId']
                )
                
                checked_count += 1
                if checked_count % 100 == 0 and self.debug:
                    print(f"DEBUG: Checked compliance locks for {checked_count}/{total_to_check} versions...")
                
                if compliance_info['locked']:
                    self.stats['compliance_locked_count'] += 1
                    
                    if not compliance_info['can_delete']:
                        future_locked_objects.append({
                            'key': object_key,
                            'version_id': version['VersionId'],
                            'retention_until': compliance_info['retention_until'],
                            'mode': compliance_info.get('mode', 'UNKNOWN')
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

    def delete_object_version(self, bucket_name, object_key, version_id, version_type):
        """Delete a specific version of an object."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete {version_type}: {object_key} (version: {version_id})")
            return True
        
        try:
            self.s3.delete_object(
                Bucket=bucket_name,
                Key=object_key,
                VersionId=version_id
            )
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
        """Delete all versions of all objects in the bucket."""
        logger.info(f"Starting deletion of {len(versions_by_object)} objects...")
        
        all_successful = True
        processed_objects = 0
        
        for object_key, versions in versions_by_object.items():
            total_versions = len(versions)
            deleted_count = 0
            processed_objects += 1
            
            if self.debug and processed_objects % 10 == 0:
                print(f"DEBUG: Processing object {processed_objects}/{len(versions_by_object)}: {object_key}")
            
            logger.info(f"Processing object: {object_key} ({total_versions} versions)")
            
            # Initialize tracking for this object
            self.deletion_status[object_key] = {
                'total_versions': total_versions,
                'deleted_versions': 0,
                'success': False
            }
            
            # Delete each version
            for version in versions:
                success = self.delete_object_version(
                    bucket_name,
                    object_key,
                    version['VersionId'],
                    version['Type']
                )
                
                if success:
                    deleted_count += 1
                    self.stats['successfully_deleted_versions'] += 1
                else:
                    self.stats['failed_deletions'] += 1
                    all_successful = False
            
            # Update tracking
            self.deletion_status[object_key]['deleted_versions'] = deleted_count
            self.deletion_status[object_key]['success'] = (deleted_count == total_versions)
            
            if self.deletion_status[object_key]['success']:
                self.stats['successfully_deleted_objects'] += 1
                logger.info(f"✓ Successfully deleted all {total_versions} versions of {object_key}")
            else:
                logger.warning(f"✗ Only deleted {deleted_count}/{total_versions} versions of {object_key}")
        
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

def main(bucket_name, endpoint_url, profile_name=None, access_key=None, secret_key=None, debug=False, dry_run=False):
    """Main function to clean up the specified bucket."""
    start_time = time.time()
    
    # Add comprehensive debugging
    if debug:
        debug_aws_configuration()
        print(f"DEBUG: Starting main() with parameters:")
        print(f"  bucket_name: {bucket_name}")
        print(f"  endpoint_url: {endpoint_url}")
        print(f"  profile_name: {profile_name}")
        print(f"  access_key provided: {bool(access_key)}")
        print(f"  secret_key provided: {bool(secret_key)}")
        print(flush=True)
    
    logger.info(f"Starting enhanced cleanup for bucket: {bucket_name}")
    
    # Initialize cleanup manager
    try:
        cleanup_manager = BucketCleanupManager(endpoint_url, profile_name, access_key, secret_key, debug, dry_run)
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
    object_count = cleanup_manager.get_object_count(bucket_name)
    cleanup_manager.stats['total_objects'] = object_count
    logger.info(f"✓ Found {object_count} objects in bucket")
    
    if object_count == 0:
        logger.info("Bucket is already empty")
        if not dry_run:
            success = cleanup_manager.delete_bucket(bucket_name)
            if success:
                logger.info("✓ Empty bucket deleted successfully")
            else:
                logger.error("✗ Failed to delete empty bucket")
        end_time = time.time()
        if debug:
            cleanup_manager.print_summary(bucket_name, start_time, end_time)
        return True
    
    # Step 3: Get all object versions
    if debug:
        print("DEBUG: Retrieving all object versions...", flush=True)
    logger.info("Retrieving all object versions...")
    versions_by_object = cleanup_manager.get_all_object_versions(bucket_name)
    total_versions = sum(len(versions) for versions in versions_by_object.values())
    cleanup_manager.stats['total_versions'] = total_versions
    logger.info(f"✓ Found {total_versions} total versions across all objects")
    
    # Step 4: Check compliance locks (optimized)
    if debug:
        print("DEBUG: Checking Object Lock configuration and compliance locks...", flush=True)
    
    can_proceed = cleanup_manager.check_bucket_compliance_locks(bucket_name, versions_by_object)
    
    if not can_proceed:
        logger.error("✗ ABORTING: Found objects with active compliance locks that prevent deletion")
        end_time = time.time()
        if debug:
            cleanup_manager.print_summary(bucket_name, start_time, end_time)
        return False
    
    logger.info("✓ No blocking compliance locks found, proceeding with deletion")
    
    # Step 5: Delete all object versions
    if not dry_run:
        logger.info("Starting object deletion process...")
        if debug:
            print("DEBUG: Starting object deletion process...", flush=True)
    else:
        logger.info("DRY RUN: Simulating object deletion process...")
        if debug:
            print("DEBUG: DRY RUN: Simulating object deletion process...", flush=True)
    
    all_deleted = cleanup_manager.cleanup_object_versions(bucket_name, versions_by_object)
    
    # Step 6: Delete bucket if all objects were successfully removed
    if all_deleted and not dry_run:
        logger.info("All objects deleted successfully, attempting to delete bucket...")
        bucket_deleted = cleanup_manager.delete_bucket(bucket_name)
    else:
        bucket_deleted = False
        if not all_deleted:
            logger.warning("Not all objects were deleted successfully, skipping bucket deletion")
        elif dry_run:
            logger.info("DRY RUN: Would attempt to delete bucket")
    
    end_time = time.time()
    
    # Step 7: Print summary
    if debug:
        cleanup_manager.print_summary(bucket_name, start_time, end_time)
    
    # Final status
    if all_deleted and (bucket_deleted or dry_run):
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
    parser.add_option("-e", "--endpoint", dest="endpoint", default=S3_URL_ENDPOINT, 
                      help=f"S3 endpoint URL (default: {S3_URL_ENDPOINT})")
    
    # Credential options
    parser.add_option("-p", "--profile", dest="profile", action="store", 
                      help="AWS profile name to use (from ~/.aws/credentials)")
    parser.add_option("--access-key", dest="access_key", action="store", 
                      help="AWS access key ID")
    parser.add_option("--secret-key", dest="secret_key", action="store", 
                      help="AWS secret access key")
    
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
    
    try:
        success = main(
            options.bucket, 
            options.endpoint, 
            options.profile,
            options.access_key,
            options.secret_key,
            options.debug, 
            options.dryrun
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