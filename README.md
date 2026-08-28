# S3 Storage Bucket Cleanup Script
## Overview

`bucketclean.py` is a tool for safely cleaning up S3 buckets in StorageGrid and other S3-compatible storage systems. It handles compliance locks, supports prefix-based filtering for targeted cleanup, and provides dry-run mode for safe testing before actual deletion.

## Features

- **Prefix Filtering**: Target specific folders/paths within a bucket (useful for TSM backup cleanup)
- **Compliance Lock Detection**: Automatically checks for Object Lock and retention policies before deletion
- **Versioning Support**: Handles versioned buckets and delete markers
- **Dry Run Mode**: Test operations without actual deletion
- **Force Mode**: Bypass governance-mode retention locks when authorized
- **Detailed Logging**: Comprehensive logs and debug output for troubleshooting
- **Multiple Auth Methods**: Supports AWS profiles, explicit credentials, or environment variables

## Installation

### Clone the Repository
```bash
git clone https://github.com/Solitarystate/s3bucketclean.git
cd s3bucketclean
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Usage

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

#### Dry run - test entire bucket cleanup
```bash
python bucketclean.py -b my-bucket-name -d -v
```

#### Dry run - test specific prefix cleanup (e.g. TSM backups)
```bash
python bucketclean.py -b my-bucket-name -x "backups/node1/" -d -v
```

#### Live execution - delete objects under a specific prefix
```bash
python bucketclean.py -b my-bucket-name -x "backups/node1/" -v
```

#### Live execution - clean entire bucket
```bash
python bucketclean.py -b my-bucket-name -v
```

## Command Line Options

| Flag | Description |
|------|-------------|
| `-b`, `--bucket` | Bucket name to clean (required) |
| `-x`, `--prefix` | Only process objects under this key prefix (e.g. 'backups/node1/') |
| `-e`, `--endpoint` | S3 endpoint URL (default: Oslo endpoint) |
| `-p`, `--profile` | AWS profile name from ~/.aws/credentials |
| `--access-key` | AWS access key ID (must use with --secret-key) |
| `--secret-key` | AWS secret access key (must use with --access-key) |
| `-d`, `--dryrun` | Dry run mode - no actual deletions performed |
| `-v`, `--debug` | Enable debug mode with detailed output |
| `--force` | Bypass governance-mode retention locks (compliance locks still block) |
| `--delete-bucket` | Delete the bucket itself after cleaning all objects |

### Prerequisites
- Python 3.7 or higher
- Valid AWS/S3 credentials configured
- Appropriate permissions for S3 operations

## Contributing

To contribute to this project:
1. Fork this repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Disclaimer

These scripts are provided as-is. Please test thoroughly in non-production environments before deploying. Ensure compliance with your organization's policies and security requirements.
Note: I've been using Claude for assistance in refining some functionalities and improving documentation of this script even though I wrote it initially. There are strong opinions on using AI tools. I respect it. 

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

**Sudeesh Varier**