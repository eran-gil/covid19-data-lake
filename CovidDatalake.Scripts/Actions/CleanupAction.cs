using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using StackExchange.Redis;

namespace CovidDataLake.Scripts.Actions
{
    internal class CleanupAction : IScriptAction
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly IConnectionMultiplexer _redisConnection;
        private readonly string? _bucketName;
        public string Name => "cleanup";

        public CleanupAction(IAmazonAdapter amazonAdapter, IConnectionMultiplexer redisConnection, BasicAmazonIndexFileConfiguration config)
        {
            _amazonAdapter = amazonAdapter;
            _redisConnection = redisConnection;
            _bucketName = config.BucketName;
        }
        public async Task<bool> Run()
        {
            try
            {
                var objects = await _amazonAdapter.ListObjectsAsync(_bucketName!, CommonKeys.INDEX_FOLDER_NAME);
                var objectList = objects.ToList();
                await Parallel.ForEachAsync(objectList, async (obj, _) =>
                {
                    await _amazonAdapter.DeleteObjectAsync(_bucketName, obj);
                });
                Console.WriteLine($"Deleted {objectList.Count} files");
                var redisEndPoint = _redisConnection.GetEndPoints().First();
                var redisServer = _redisConnection.GetServer(redisEndPoint);
                await redisServer.FlushAllDatabasesAsync();
                Console.WriteLine("Cleaned Redis");
            }
            catch
            {
                //ignore
            }

            return true;
        }
    }
}
