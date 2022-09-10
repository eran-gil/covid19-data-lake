# CovidDataLake.MetadataIndexer
The metadata indexer executable
### Environment Variables
* `AWS_ACCESS_KEY_ID` - The access key to the relevant AWS account
* `AWS_SECRET_ACCESS_KEY` - The secret access key to the relevant AWS account
* `AmazonGeneralConfig__ServiceUrl` - The S3 service url for the relevant region, e.g.: `https:////s3.us-east-2.amazonaws.com`
* `AmazonIndexFile__BucketName` - The bucket name of the index files in the AWS S3 account
* `CountMinSketch__BucketName` - The bucket name of the Count-Min-Sketch index file in the AWS S3 account
* `HyperLogLog__BucketName` - The bucket name of the HyperLogLog index file in the AWS S3 account