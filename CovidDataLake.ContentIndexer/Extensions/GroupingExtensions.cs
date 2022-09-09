using System.Collections.Generic;
using System.Linq;

namespace CovidDataLake.ContentIndexer.Extensions
{
    public static class GroupingExtensions
    {
        public static IEnumerable<TV> GetAllValues<T, TV>(this IEnumerable<KeyValuePair<T, IEnumerable<TV>>> group)
        {
            return group.SelectMany(kvp => kvp.Value);
        }
    }
}
