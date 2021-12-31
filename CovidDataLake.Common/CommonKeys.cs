﻿// ReSharper disable InconsistentNaming
namespace CovidDataLake.Common
{
    public class CommonKeys
    {
        public const string ROOT_INDEX_FILE_LOCK_KEY = "::ROOT_INDEX_FILE_LOCK::";
        public const string END_OF_INDEX_FLAG = "__END_OF_INDEX__";
        public const string INDEX_FOLDER_NAME = "__INDEX__";
        public const string COLUMN_INDICES_FOLDER_NAME = "__COLUMNS__";
        public const string METADATA_INDICES_FOLDER_NAME = "__METADATA__";
        public const string CONTENT_FOLDER_NAME = "__CONTENT__";
        public const string TEMP_FOLDER_NAME = "__TEMP__";
    }
}
