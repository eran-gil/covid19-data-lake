namespace CovidDataLake.Common
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<T> ToTaskResults<T>(this IEnumerable<Task<T>> tasks)
        {
            return tasks.Select(t => t.Result).Where(result => result != null);
        }
    }
}
