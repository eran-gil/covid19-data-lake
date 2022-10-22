using System.Threading.Channels;

namespace CovidDataLake.Common
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<T> ToTaskResults<T>(this IEnumerable<Task<T>> tasks)
        {
            return tasks.Select(t => t.Result).Where(result => result != null);
        }

        public static IEnumerable<T> NotNull<T>(this IEnumerable<T> enumerable)
        {
            return enumerable.Where(t => t != null);
        }

        public static IEnumerable<string> NotEmptyOrNull(this IEnumerable<string> enumerable)
        {
            return enumerable.Where(t => !string.IsNullOrEmpty(t));
        }

        public static IAsyncEnumerable<T> NotNull<T>(this IAsyncEnumerable<T> enumerable)
        {
            return enumerable.Where(t => t != null);
        }

        public static IAsyncEnumerable<string> NotEmptyOrNull(this IAsyncEnumerable<string> enumerable)
        {
            return enumerable.Where(t => !string.IsNullOrEmpty(t));
        }

        public static async Task WriteRangeAsync<T>(this Channel<T> channel, IAsyncEnumerable<T> range)
        {
            await range.ForEachAwaitAsync(async item => await channel.Writer.WriteAsync(item));
        }

        public static async Task<T?> NthItemOrLast<T>(this IAsyncEnumerator<T?> enumerator, int n, T? defaultValue=default)
        {
            var item = defaultValue;
            for (var i = 0; i < n; i++)
            {
                if (!(await enumerator.MoveNextAsync().ConfigureAwait(false)))
                {
                    break;
                }

                item = enumerator.Current;
            }

            return item;
        }
    }
}
