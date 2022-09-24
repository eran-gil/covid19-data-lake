namespace CovidDataLake.Common.Files
{
    public static class FileCreator
    {
        public static FileStream CreateFileAndPath(string filepath)
        {
            CreateDirectoryFromFilePath(filepath);
            var file = File.Create(filepath);
            return file;
        }

        public static FileStream OpenFileWriteAndCreatePath(string filePath)
        {
            CreateDirectoryFromFilePath(filePath);
            var file = File.OpenWrite(filePath);
            return file;
        }

        private static void CreateDirectoryFromFilePath(string filepath)
        {
            var directoryPath = Path.GetDirectoryName(filepath);
            if (string.IsNullOrEmpty(directoryPath) || string.IsNullOrWhiteSpace(directoryPath)) return;
            Directory.CreateDirectory(directoryPath);
        }
    }
}
