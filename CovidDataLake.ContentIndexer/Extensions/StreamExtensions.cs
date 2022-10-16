using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace CovidDataLake.ContentIndexer.Extensions
{
    public static class StreamExtensions
    {
        public static IEnumerable<T> GetDeserializedRowsFromFile<T>(this FileStream file, long beginOffset, long offsetLimit)
        {
            var readLength = (int) offsetLimit - beginOffset;
            var buffer = new byte[readLength];
            file.Seek(beginOffset, SeekOrigin.Begin);
            // ReSharper disable once MustUseReturnValue
            file.Read(buffer);
            using var rawDataStream = new MemoryStream(buffer);
            using var streamReader = new StreamReader(rawDataStream);
            var values = new List<T>();
            while (!streamReader.EndOfStream)
            {
                var currentLine = streamReader.ReadLine();
                var currentValue = JsonConvert.DeserializeObject<T>(currentLine!);
                if (currentValue == null)
                {
                    throw new InvalidDataException("The index is not in the expected format");
                }

                values.Add(currentValue);
            }
            return values;
        }

        public static async Task WriteObjectsToFileAsync<T>(this JsonTextWriter jsonWriter, IEnumerable<T> indexValues)
        {
            var serializer = new JsonSerializer();
            foreach (var indexValue in indexValues)
            {
                serializer.Serialize(jsonWriter, indexValue);
                await jsonWriter.WriteWhitespaceAsync(Environment.NewLine);
            }
        }

        public static async Task WriteObjectToLineAsync<T>(this StreamWriter streamWriter, T indexValue)
        {
            if (streamWriter == null) throw new ArgumentNullException(nameof(streamWriter));
            var serialized = JsonConvert.SerializeObject(indexValue);
            await streamWriter.WriteLineAsync(serialized);
        }

        public static long ReadBinaryLongFromStream(this Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            using var binaryReader = new BinaryReader(stream, Encoding.Default, true);
            var longData = binaryReader.ReadInt64();

            return longData;
        }

        public static void WriteBinaryLongsToStream(this Stream stream, IEnumerable<long> longData)
        {
            using var binaryWriter = new BinaryWriter(stream, Encoding.Default, true);
            foreach (var l in longData)
            {
                binaryWriter.Write(l);
            }
        }

        public static IEnumerable<string> ReadLines(this StreamReader reader)
        {
            while (!reader.EndOfStream)
            {
                var value = reader.ReadLine();
                yield return value;
            }
        }
    }
}
