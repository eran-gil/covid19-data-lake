// ReSharper disable InconsistentNaming
namespace CovidDataLake.Common
{
    public class CommonKeys
    {
        public const string ROOT_INDEX_FILE_LOCK_KEY = "::ROOT_INDEX_FILE_LOCK::";
        public const string ROOT_INDEX_UPDATE_FILE_LOCK_KEY = "::ROOT_INDEX_FILE_UPDATE_LOCK::";
        public const string END_OF_INDEX_FLAG = "__END_OF_INDEX__";
        public const string INDEX_FOLDER_NAME = "__INDEX__";
        public const string COLUMN_INDICES_FOLDER_NAME = "__COLUMNS__";
        public const string METADATA_INDICES_FOLDER_NAME = $"{INDEX_FOLDER_NAME}/__METADATA__";
        public const string CONTENT_FOLDER_NAME = "__CONTENT__";
        public const string TEMP_FOLDER_NAME = "__TEMP__";
        public const string HLL_FOLDER_NAME = "HLL_CARDINALITY";
        public const string HLL_FILE_TYPE = "hll";
        public const string CMS_FOLDER_NAME = "CMS_FREQUENCY";
        public const string CMS_FILE_TYPE = "cms";
    }
}
