# Datalake framework for analysis of the air polution situation in the vicinity of Belgrade, RS

## Goal of the project

This is a pet project. Main goal is for me to get my hands as dirty as I can with the insides of Databricks, Spark, Python, and everything else associated with the profession of Data Engineer.

I will have to see as I go whether this projects steers itself into something that can be useful to others.

## Data used in the analysis

I will use data, publicly available for free from the following sources:

*   Air quality data from [OpenAQ](https://openaq.org)

> OpenAQ API provides data from multiple sensors operated by third parties. For my purposes I retrieve data from all sensors within the 8 km radius of the Belgrade center for the past 5 years.  
> The data is retrieved in JSON format, and to significantly save on storage space (I use Databricks Free Edition, so I suppose there is a limit to what I can store for free), I gzip every file before I store it on disk. This reduces the size of the raw data aproximately 50-fold.

*   Weather data from [Open-meteo](https://open-meteo.com/)

> Open-meteo provides temperature, wind speed and direction, atmospheric pressure, and other weather-related data, which is useful in my research. All that data is available via an API, capable of handling the 5 years of historic data in a single API call.  
> Results are retrieved in JSON format, and stored gzipped.

## Data architecture

I will use the **Medalion Architecture** to gradually improve data quality and ensure the pipeline idempotency. The following picture illustrates the data flow:  
!\[for the Medallion Architecture data flow: raw data from OpenAQ and Open-meteo APIs flows through landing and bronze layers for ingestion, then to silver layer for deduplication and normalization, and finally to gold layer for business aggregates used in analysis\](Data flow diagram.drawio.png)

1.  _Setup the ingestion job_: depending on whether this is a backfill or an incremental data load operation, a SQL script is run to determine the parameters of the Lakeflow job.
2.  _Retrieve the raw data_: a Lakeflow job runs a Python script to call the external API and retrieve the raw data.
3.  _Store the raw data_: the raw data is stored in a landing volume within the `00_landing` schema.
4.  _Ingest data into_ `bronze` _tables_: a Lakeflow declariative pipeline is run to ingest the raw data into a Delta table and perform initial transformations. The tables are stored in `01_bronze` schema and Lakeflow pipeline expectations are applied to start improving the data quality.
5.  _Deduplicate data and move to_ `silver`: a Lakeflow job runs a streaming query against the bronze data, for each record generates a unique ID, and deduplicates the data. Result is stored in a Delta table in the `02_silver` schema. The information about weather and polition sensors, locations, and measured parameters is extracted, the data is normalized. 
6.  _Compute business aggregates_: a Lakeflow declarative pipeline processes the data to prepare daily and hourly aggregates, denormalize the data for easy consumption. Resulting aggregates are stored in the `03_gold` layer and used for data analysis.

## Architectural decisions

*   All code is packaged as a Databricks Asset Bundle to facilitate software engineering best practices like source control, and compatibility with continuous integration and delivery (CI/CD) principles.
*   All artifacts including the source code, job and pipeline definitions, are source controlled in git.
*   Databricks Auto Loader is used to ingest the raw data to efficiently process new data and make use of schema evolutions.
*   When moving data between Medalion Architecture layers, data quality checks are implemented with expectations to steadily improve data quality.