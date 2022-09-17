# covid19-data-lake
This project intends on creating a data lake using AWS S3 that has the following indices:
* File-based content index based on the following [article](https://www.semanticscholar.org/paper/Needle-in-a-haystack-queries-in-cloud-data-lakes-Weintraub-Gudes/1f5b9de16302525ab07e50056f3a8af565fd131a). 
* HyperLogLog metadata index to indicate the approximate number of unique values of a metadata field
* Count-Min-Sketch metadata index to indicate the approximate number of repetitions per each value of a metadata field

The data lake is accompanied by an API to do the following:
* Upload a file to the data lake and start the indexing process
* Query for content based on the content-index
* Query for metadata statistics based on the metadata indices

### Project Structure
|Project Name|Purpose|
|---|---|
|CovidDataLake.Cloud|Common code to access the cloud resources of the data lake|
|CovidDataLake.Common|Common code that is shared between all of the services|
|CovidDataLake.ContentIndexer|The engine that indexes the contents of files in the data lake|
|CovidDataLake.MetadataIndexer|The engine that indexes the metadata of files in the data lake|
|CovidDataLake.Pubsub|Common code to publish and subscribe to events in the ETL process|
|CovidDataLake.Queries|The business-logic of the queries performed on the data lake|
|CovidDataLake.Storage|Common code to handle usage of local disk storage|
|CovidDataLake.WebAPI|The API for the data lake, includes updates and queries|

### Getting Started Requirements
* [.NET 6.0](https://dotnet.microsoft.com/en-us/download/dotnet/6.0) installed
* [Redis server](https://redis.io/download/) running and configured correctly in all relevant `appsettings.json` files in the following way:
```json
{
    "Redis": "[HOSTNAME]:[PORT],connectTimeout=15000,syncTimeout=15000"
}
```
* [Kafka](https://kafka.apache.org/downloads) cluster running with all instances configured correctly in all relevant `appsettings.json` files in the following way:
```json
{
    "Kafka": {
        "Instances": [
            {
                "Host": "[HOST_NAME]",
                "Port": 9092
            }
        ],
        "Topic": "[TOPIC_NAME]",
        "GroupId": "[CONSUMER_GROUP_ID]" //this is used only for consuming projects (aka indexing engines)
}
```
* Project-specific requirements are listed inside each project's folder
