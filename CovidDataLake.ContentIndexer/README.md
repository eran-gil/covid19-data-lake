# CovidDataLake.ContentIndexer
The content indexer executable
### Environment Variables
* `AWS_ACCESS_KEY_ID` - The access key to the relevant AWS account
* `AWS_SECRET_ACCESS_KEY` - The secret access key to the relevant AWS account
* `AmazonGeneralConfig__ServiceUrl` - The S3 service url for the relevant region, e.g.: `https:////s3.us-east-2.amazonaws.com`
* `AmazonIndexFile__BucketName` - The bucket name of the index files in the AWS S3 account
* `AmazonRootIndex__BucketName` - The bucket name of the root index file (content-index) in the AWS S3 account
