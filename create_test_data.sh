#!/bin/bash
# Script to create test data in an S3 bucket with multiple prefixes
# Usage: ./create_test_data.sh <bucket-name> <endpoint-url> [profile-name]

set -e

BUCKET_NAME=$1
ENDPOINT_URL=$2
PROFILE_NAME=${3:-default}

if [ -z "$BUCKET_NAME" ] || [ -z "$ENDPOINT_URL" ]; then
    echo "Usage: $0 <bucket-name> <endpoint-url> [profile-name]"
    echo "Example: $0 my-test-bucket https://ep.s3-no.basefarm-orange.com storagegrid"
    exit 1
fi

echo "Creating test data in bucket: $BUCKET_NAME"
echo "Endpoint: $ENDPOINT_URL"
echo "Profile: $PROFILE_NAME"
echo ""

# Function to create a test object
create_object() {
    local key=$1
    local content=$2
    echo "$content" | aws s3 cp - "s3://${BUCKET_NAME}/${key}" \
        --endpoint-url "$ENDPOINT_URL" \
        --profile "$PROFILE_NAME"
    echo "✓ Created: $key"
}

# Create test data under different prefixes
echo "Creating test data under prefix: test-prefix-a/"
create_object "test-prefix-a/file1.txt" "Test content for file1 in prefix-a"
create_object "test-prefix-a/file2.txt" "Test content for file2 in prefix-a"
create_object "test-prefix-a/subdir/file3.txt" "Test content for file3 in prefix-a/subdir"

echo ""
echo "Creating test data under prefix: test-prefix-b/"
create_object "test-prefix-b/file1.txt" "Test content for file1 in prefix-b"
create_object "test-prefix-b/file2.txt" "Test content for file2 in prefix-b"

echo ""
echo "Creating test data under prefix: backups/node1/"
create_object "backups/node1/backup1.dat" "Simulated TSM backup file 1"
create_object "backups/node1/backup2.dat" "Simulated TSM backup file 2"
create_object "backups/node1/2026/08/backup3.dat" "Simulated TSM backup file 3"

echo ""
echo "Creating test data under prefix: backups/node2/"
create_object "backups/node2/backup1.dat" "Simulated TSM backup file for node2"
create_object "backups/node2/backup2.dat" "Simulated TSM backup file for node2"

echo ""
echo "Creating test data at root level (no prefix)"
create_object "root-file1.txt" "Root level file 1"
create_object "root-file2.txt" "Root level file 2"

echo ""
echo "=========================================="
echo "Test data creation complete!"
echo "=========================================="
echo ""
echo "Created structure:"
echo "  test-prefix-a/           (3 objects)"
echo "  test-prefix-b/           (2 objects)"
echo "  backups/node1/           (3 objects)"
echo "  backups/node2/           (2 objects)"
echo "  root level               (2 objects)"
echo "  TOTAL:                   12 objects"
echo ""
echo "You can now test the prefix filtering with:"
echo ""
echo "  # Test prefix filtering (dry run):"
echo "  python3 bucketclean.py -b $BUCKET_NAME -x 'test-prefix-a/' -d -v -e $ENDPOINT_URL -p $PROFILE_NAME"
echo ""
echo "  # Test full bucket scan (dry run):"
echo "  python3 bucketclean.py -b $BUCKET_NAME -d -v -e $ENDPOINT_URL -p $PROFILE_NAME"
echo ""
echo "  # Actually delete objects under a prefix:"
echo "  python3 bucketclean.py -b $BUCKET_NAME -x 'test-prefix-a/' -v -e $ENDPOINT_URL -p $PROFILE_NAME"
