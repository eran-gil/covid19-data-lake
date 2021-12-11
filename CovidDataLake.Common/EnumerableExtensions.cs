using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CovidDataLake.Common
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<T> ToTaskResults<T>(this IEnumerable<Task<T>> tasks)
        {
            return tasks.Select(t => t.Result).Where(result => result != null);
        }

        public static async IAsyncEnumerable<IAsyncEnumerable<T>> Batch<T>(
            this IAsyncEnumerable<T> source, int batchSize)
        {
            var enumerator = source.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync())
                yield return YieldBatchElements(enumerator, batchSize - 1);
        }

        private static async IAsyncEnumerable<T> YieldBatchElements<T>(
            IAsyncEnumerator<T> source, int batchSize)
        {
            yield return source.Current;
            for (var i = 0; i < batchSize && await source.MoveNextAsync(); i++)
                yield return source.Current;
        }
    }
}
