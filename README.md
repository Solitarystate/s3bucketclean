# S3 Storage Bucket Cleanup Scripts
## Overview

The bucketclean.py script is a comprehensive tool designed to safely clean up S3 buckets in AWS S3 and AWS S3 compliant storage environments. It provides intelligent compliance lock checking, detailed progress tracking, and multiple safety features to ensure safe bucket cleanup operations.

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

#### Dry run (recommended first step)
```bash
python bucketclean.py -b my-bucket-name -d -v
```
#### Live execution
```bash
python bucketclean.py -b my-bucket-name -v
```

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

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

**Sudeesh Varier**