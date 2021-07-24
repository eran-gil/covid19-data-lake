using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Utf8Json;

namespace CovidDataLake.ContentIndexer.Extensions
{
    public static class StreamExtensions
    {
        public static async IAsyncEnumerable<T> GetDeserializedRowsFromFileAsync<T>(this FileStream file, long offsetLimit)
        {
            using var streamReader = new StreamReader(file);
            while (file.Position < offsetLimit)
            {
                var currentLine = await streamReader.ReadLineAsync();
                var currentValue = JsonSerializer.Deserialize<T>(currentLine);
                if (currentValue == null)
                {
                    throw new InvalidDataException("The index is not in the expected format");
                }

                yield return currentValue;
            }
        }

        public static async Task WriteObjectToLineAsync<T>(this StreamWriter streamWriter, T indexValue)
        {
            if (streamWriter == null) throw new ArgumentNullException(nameof(streamWriter));
            await JsonSerializer.SerializeAsync(streamWriter.BaseStream, indexValue);
            await streamWriter.WriteLineAsync("");
        }

        public static long ReadBinaryLongFromStream(this Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            using var binaryReader = new BinaryReader(stream);
            var longData = binaryReader.ReadInt64();

            return longData;
        }

        public static void WriteBinaryLongToStream(this Stream stream, long longData)
        {
            using var binaryWriter = new BinaryWriter(stream);
            binaryWriter.Write(longData);
        }
    }
}
