# Spotify End-to-End Azure Data Engineering Project — ADF

## Project Overview

This repository contains the Azure Data Factory (ADF) implementation of an end-to-end Spotify data engineering project. The project demonstrates how to orchestrate data ingestion from Azure SQL Database into Azure Data Lake Storage Gen2 (ADLS Gen2), where the data can be processed and transformed using Azure Databricks.

The overall solution follows a medallion lakehouse architecture with Bronze, Silver, and Gold layers.

## Architecture

```text
Azure SQL Database
        |
        v
Azure Data Factory
        |
        v
Azure Data Lake Storage Gen2
        |
        v
   Bronze Layer
        |
        v
 Azure Databricks
        |
        v
   Silver Layer
        |
        v
    Gold Layer
        |
        v
Analytics and Reporting
```

## Technologies Used

* **Azure Data Factory:** Pipeline orchestration and data ingestion.
* **Azure SQL Database:** Source database containing Spotify-related data.
* **Azure Data Lake Storage Gen2:** Scalable cloud storage for the data lake.
* **Azure Databricks:** Data transformation and lakehouse processing.
* **GitHub:** Source control and project documentation.

## Source Data

The project works with the following Spotify-related tables:

* `DimUser`
* `DimArtist`
* `DimDate`
* `DimTrack`
* `FactStream`

These tables represent user, artist, date, track, and streaming activity data.

## Key Features

### 1. Data Ingestion and Orchestration

* Configured Azure Data Factory to orchestrate data movement from Azure SQL Database to ADLS Gen2.
* Organized pipeline components to support the ingestion workflow.
* Integrated ADF with Azure storage and downstream Databricks processing.

### 2. Data Lake Integration

* Used ADLS Gen2 as the central storage layer for ingested data.
* Organized data into the Bronze, Silver, and Gold lakehouse layers.
* Enabled downstream processing through Azure Databricks.

### 3. Databricks Integration

Azure Data Factory forms the ingestion and orchestration component of the overall solution. The separate Databricks implementation handles data cleansing, deduplication, Delta Lake processing, and Gold-layer transformations.

### 4. Source Control

* Maintained the ADF implementation in GitHub.
* Documented the project architecture and implementation.
* Kept the ADF and Databricks components in separate repositories for easier organization.

## End-to-End Data Flow

1. **Source:** Spotify-related data is stored in Azure SQL Database.
2. **Ingestion:** Azure Data Factory orchestrates the movement of source data.
3. **Landing and Bronze:** Data is stored in ADLS Gen2 for downstream processing.
4. **Silver:** Azure Databricks cleans and transforms the ingested data.
5. **Gold:** Curated data is prepared for analytics and reporting.
6. **Consumption:** Gold-layer data can be used by reporting tools such as Power BI.

## Related Repository

The Databricks transformations and lakehouse implementation are maintained in a separate repository:

**[Spotify Azure Data Engineering — Databricks Repository](https://github.com/MichaelJohnsonlmj/spotify-azure-data-engineering)**

## Project Objectives

* Develop practical experience with Azure Data Factory.
* Understand cloud data ingestion and orchestration.
* Integrate Azure SQL Database with ADLS Gen2.
* Understand the responsibilities of Bronze, Silver, and Gold layers.
* Practice source control and documentation for data engineering projects.
* Build a portfolio project demonstrating Azure data engineering concepts.

## Security Considerations

* Do not commit passwords, access keys, tokens, or connection strings.
* Store credentials securely using appropriate Azure security services.
* Use managed identities and least-privilege access where supported.
* Use synthetic or sample data for public demonstrations.

## Author

**Michael Johnson Louis**

Azure Data Engineering | SQL | Python | Azure Data Factory | Azure Databricks | PySpark | Delta Lake
