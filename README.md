# Data Pipeline Project: From APIs to Visualization

## Project Overview
This project implements a complete big data pipeline using modern technologies:
- **Data Ingestion**: API extraction to Kafka
- **Processing**: Spark for real-time transformations
- **Storage**: PostgreSQL database
- **Visualization**: Apache Superset dashboards

<img width="900" alt="image" src="https://github.com/user-attachments/assets/2bcad29f-5fa2-438a-a146-c062ed1ad55f" />

All components are containerized with Docker for easy deployment.

## Technology Stack

| Component       | Technology   | Version |
|-----------------|-------------|---------|
| Data Bus        | Apache Kafka | 3.2.0   |
| Processing      | Apache Spark | 3.3.0   |
| Storage         | PostgreSQL   | 14.5    |
| Visualization   | Superset     | 2.0.0   |
| Orchestration   | Docker       | 20.10+  |

## Getting Started

### Prerequisites
- Docker Engine (20.10.0+)
- Docker Compose (1.29.0+)
- 4GB+ available RAM
- Python 3.8+ (for script development)

### Installation
```bash
git clone https://github.com/your-repo/data-pipeline.git
cd data-pipeline
```
### Running the Pipeline
**Start all services:**
```
bash
docker-compose up -d
```
**Initialize the environment:**
```
bash
# Install dependencies
docker-compose exec spark-master pip install -r /requirements.txt

# Create Superset admin
docker-compose exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin

# Initialize Superset
docker-compose exec superset superset init
```
**Start data processing:**
```
bash
./scripts/start_pipeline.sh
```
## Project Structure
```
.
├── config/
│   ├── init-db.sql             # Database schema
│   └── superset_config.py      # Superset configuration
├── scripts/
│   ├── python/                 # Processing scripts
│   │   ├── producers/          # Data ingestion
│   │   └── processors/         # Spark jobs
│   └── start_pipeline.sh       # Launch script
├── docker-compose.yml          # Service definitions
└── README.md                   # This file
```
## Accessing Services
Service	    URL/Connection	        Credentials
Superset	  http://localhost:8088	  admin/admin
PostgreSQL	postgres:5432	          postgres_user/password
Kafka	      broker:9092             -
## Example Queries
### Product Analysis
```
sql
SELECT 
  category,
  AVG(price) as avg_price,
  COUNT(*) as product_count,
  AVG(rating) as avg_rating
FROM products
GROUP BY category
ORDER BY avg_price DESC;
```
### Cart Analytics
```
sql
SELECT 
  day_of_week,
  COUNT(*) as cart_count,
  AVG(total_items) as avg_items
FROM vw_cart_analysis
GROUP BY day_of_week;
```
### Creating Visualizations
**Connect Superset to your PostgreSQL database:**

Host: postgres

Database: data_fakestore_db

Username: postgres_user

Password: postgres_password

**Recommended Charts:**

Product distribution by category (Bar Chart)

Price vs Rating correlation (Scatter Plot)

Weekly cart activity (Time Series)

## Monitoring
View logs for any service:
```
bash
docker-compose logs -f [service_name]
```
Key log files:
```
logs/producer.log - API ingestion

logs/spark_products.log - Product processing

logs/spark_carts.log - Cart analysis
```
## Shutting Down
To stop all services while preserving data:
```
bash
docker-compose down
```
To completely reset (deletes all data):
```
bash
docker-compose down -v
```
## Troubleshooting
### Common Issues:

Port conflicts: Ensure ports 8088 (Superset) and 5432 (PostgreSQL) are free

Memory errors: Increase Docker's memory allocation to 6GB+

Kafka connection issues: Wait 2-3 minutes after startup for services to initialize

## License
MIT License - See LICENSE for details.

