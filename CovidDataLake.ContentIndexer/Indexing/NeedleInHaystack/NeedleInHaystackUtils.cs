using System;
using CovidDataLake.Common;

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack
{
    public static class NeedleInHaystackUtils
    {
        public static string CreateNewColumnIndexFileName(string columnName)
        {
            return $"{CommonKeys.INDEX_FOLDER_NAME}/{CommonKeys.COLUMN_INDICES_FOLDER_NAME}/{columnName}/{Guid.NewGuid()}.txt";
        }
    }
}
