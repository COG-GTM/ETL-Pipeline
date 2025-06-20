# ETL Pipeline - Comprehensive Vehicle Sales Data Processing Guide

A comprehensive ETL (Extract, Transform, Load) pipeline for processing vehicle sales and service data from PostgreSQL databases to Amazon S3 storage.

## 🏗️ Architecture Overview

This ETL pipeline processes automotive dealership data through three main stages:

1. **Extract**: Connects to PostgreSQL database and retrieves vehicle sales and service data
2. **Transform**: Identifies and removes duplicate records from the dataset
3. **Load**: Uploads cleaned data as CSV files to Amazon S3 storage

### Data Flow Diagram
```
PostgreSQL Database → Data Extraction → Duplicate Removal → S3 CSV Upload
     (Source)           (extract.py)      (transform.py)    (load_data_to_s3.py)
```

## 📊 Database Schema

The pipeline processes data from four main tables:

- **dealerships**: Dealership information (id, name, region)
- **vehicles**: Vehicle inventory (vin, model, year, dealership_id)
- **sales_transactions**: Sales records (id, vin, sale_date, sale_price, buyer_name)
- **service_records**: Service history (id, vin, service_date, service_type, service_cost)

## 🔧 Components

### Core Modules

- **`src/extract.py`** - PostgreSQL database connection and data extraction
  - Connects to PostgreSQL using psycopg2
  - Executes complex JOIN queries across vehicle, dealership, sales, and service tables
  - Handles date formatting and null value replacement

- **`src/transform.py`** - Data transformation and cleaning
  - Identifies duplicate records across configurable column subsets
  - Removes duplicates while preserving data integrity
  - Provides detailed logging of transformation results

- **`src/load_data_to_s3.py`** - Amazon S3 data loading
  - Establishes secure S3 connections using boto3
  - Converts pandas DataFrames to CSV format
  - Uploads processed data to specified S3 buckets

- **`main.py`** - Pipeline orchestration
  - Coordinates the complete ETL workflow
  - Manages environment configuration
  - Provides execution timing and progress tracking

## ⚙️ Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# Database Configuration
DB_NAME=your_database_name
DB_HOST=your_database_host
DB_PORT=5432
DB_USER=your_database_user
DB_PASSWORD=your_database_password

# AWS Configuration
aws_access_key_id=your_aws_access_key
aws_secret_access_key_id=your_aws_secret_key
```

### Dependencies

The pipeline requires the following Python packages:
- `psycopg2-binary` - PostgreSQL database adapter
- `pandas` - Data manipulation and analysis
- `boto3` - AWS SDK for Python
- `python-dotenv` - Environment variable management

## 🚀 Deployment Options

### Option A: Command Line Interface

#### Requirements
- Python 3.8+
- pip package manager
- Access to PostgreSQL database
- AWS credentials configured

#### Setup Instructions

1. **Clone the repository**
```bash
git clone <repository-url>
cd ETL-Pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment**
```bash
cp .env.example .env
# Edit .env file with your database and AWS credentials
```

4. **Run the pipeline**
```bash
python main.py
```

### Option B: Docker Deployment

#### Requirements
- Docker Engine installed
- Docker Compose (optional, for local PostgreSQL)

#### Setup Instructions

1. **Build the Docker image**
```bash
docker build -t etl-pipeline .
```

2. **Configure environment**
```bash
cp .env.example .env
# Edit .env file with your credentials
```

3. **Run with external database**
```bash
docker run --env-file .env etl-pipeline
```

4. **Run with local PostgreSQL (using Docker Compose)**
```bash
# Start PostgreSQL service
docker-compose up -d postgres

# Run ETL pipeline
docker run --env-file .env --network etl-pipeline_default etl-pipeline
```

## 🗄️ Local Development with PostgreSQL

For local development and testing, use the included Docker Compose configuration:

```bash
# Start local PostgreSQL with sample data
docker-compose up -d postgres

# The database will be initialized with sample data from init/init.sql
# Connection details:
# - Host: localhost
# - Port: 5432
# - Database: etl_db
# - Username: postgres
# - Password: mysecretpassword
```

## 📈 Pipeline Output

The pipeline generates the following outputs:

- **S3 Object**: `s3://cognition-devin/auto_oem/etl/vehicle_sales_deduped.csv`
- **Format**: CSV with headers
- **Content**: Deduplicated vehicle sales and service data
- **Columns**: vin, model, year, dealership_name, region, sale_date, sale_price, buyer_name, service_date, service_type, service_cost

## 🔍 Monitoring and Logging

The pipeline provides comprehensive logging:
- Database connection status
- Record extraction counts
- Duplicate detection and removal statistics
- S3 upload confirmation
- Total execution time tracking

## 🛠️ Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify database credentials in `.env` file
   - Ensure PostgreSQL server is accessible
   - Check network connectivity and firewall settings

2. **S3 Upload Failures**
   - Validate AWS credentials and permissions
   - Confirm S3 bucket exists and is accessible
   - Check AWS region configuration

3. **Docker Issues**
   - Ensure Docker daemon is running
   - Verify `.env` file is properly configured
   - Check Docker network connectivity for database access

## 📋 Technical Specifications

- **Python Version**: 3.8+
- **Database**: PostgreSQL 15+
- **Cloud Storage**: Amazon S3
- **Data Format**: CSV
- **Container Runtime**: Docker
- **Architecture**: Modular ETL design

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

[Add your license information here]
