# Data-Engineering-Project using Indian Rainfall Data API
An end-to-end data engineering pipeline on Databricks leveraging the publicly available RainFall API. This project covers:
- Data Ingestion (Bronze)
- Data Processing & Cleaning (Silver)
- Data Quality & Delivery (Gold)

**Medallion Layers**:

| Layer  | Purpose                                                    |
| ------ | ---------------------------------------------------------- |
| Bronze | Ingest raw data from API into Parquet                     |
| Silver | Clean, dedupe, enrich; enforce schemas with PySpark       |
| Gold   | Spliting the date as Fact & Dimension Table               |

---
# PROJECT ARCHITECTURE
<img width="968" height="189" alt="image" src="https://github.com/user-attachments/assets/6c975294-b294-4fb7-a385-517110d1dfc0" />

### Phase 1: Bronze Layer (Raw Ingestion)

- **Sources**  
  - "District-wise Rainfall Distribution" as API Request
  - Source from "https://ndap.niti.gov.in/dataset/7319"
<img width="1880" height="803" alt="image" src="https://github.com/user-attachments/assets/60f688f4-74ba-42a3-9af0-67e560d207e6" />

    
- **Storage**  
  - All raw ingestions stored as Parquet in the `rainfall_data/bronze_layer` container
 
### Phase 2: Silver (Cleansing & Enrichment)

- **Transformations**  
  - Split multi-valued columns (e.g., Daily Actual)  
  - Remove duplicates
  - Cast of data types for analytics readiness
    
- **Storage**  
  - Cleaned Parquet files in the `rainfall_data/silver_layer` container

### Phase 3: Gold (Quality & Aggregation)

- **Transformations**  
  - Removed Unncessary columns
  - De-normalizing dataset to tables(Fact & Dimension)
  - Re-naming columns for ease of use

- **Output**  
  - Aggregated the tables as `rainfall_data.rain_fact_table` and `workspace.rainfall_data.state_table`
 
---

## Technology Stack

| Component                 | Purpose                                   |
| ------------------------- | ----------------------------------------- |
| Databricks                | Spark-based ETL & Delta Live Tables       |
| Unity Catlog              | ACID-compliant, performant data format    |
| Python / PySpark          | Data transformation logic                 |

