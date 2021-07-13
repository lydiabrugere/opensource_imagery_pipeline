# An open source imagery pipeline for given AOI
A modularized pipeline to utilize raw surface reflectance from satellite imagery to derive insights for vegetation monitoring. Mian features include:  

1. Batch processing and streaming of multiple satellite sources (Landsat-7, Landsat-8 and Sentinel-2 satellites) from open data on AWS and GCP
1. Performed image processing, geoprocessing to serve image mosaics for analytics on the cloud
1. Multiple AWS services (EFS, S3, EC2, lambda etc.), Rundeck, Apache Superset are leveraged to manage and visualize the service

### Pipeline Flowchart

![flowchart](https://github.com/lydiabrugere/opensource_imagery_pipeline/blob/main/flowchart.png)