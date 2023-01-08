# Covid-19 ETL with Pyspark
### by Nham Nguyen Xuan

## Introduction
In this project, I decided to use the Covid-19 Dataset provided by Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE). You can find more details in this data (repo)['https://github.com/CSSEGISandData/COVID-19'].
Predicting churn rates is a challenging and common problem that data scientists and analysts regularly encounter in any customer-facing business. Additionally, the ability to efficiently manipulate large datasets with Spark is one of the highest-demand skills in the field of data.  
Here, I will use only 3 files: `time_series_covid19_confirmed_global.csv`, `time_series_covid19_deaths_global.csv` and `time_series_covid19_recovered_global.csv`.  
With these 3 files, I will merge them together and cleaning them to produce the first table to write back to S3 Output Bucket. I will perform one aggregation to create the last table and write back to S3 Output Bucket for future analysis.
