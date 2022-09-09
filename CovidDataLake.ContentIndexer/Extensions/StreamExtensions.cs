using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Jil;

namespace CovidDataLake.ContentIndexer.Extensions
{
    public static class StreamExtensions
    {
        public static IEnumerable<T> GetDeserializedRowsFromFileAsync<T>(this FileStream file, long offsetLimit)
        {
            using var streamReader = new StreamReader(file, Encoding.Default, true, 1024, true);
            while (file.Position < offsetLimit)
            {
                var currentLine = streamReader.ReadLine();
                var currentValue = JSON.Deserialize<T>(currentLine);
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
            var serialized = JSON.Serialize(indexValue);
            await streamWriter.WriteLineAsync(serialized);
        }

        public static void WriteObjectToLine<T>(this StreamWriter streamWriter, T indexValue)
        {
            if (streamWriter == null) throw new ArgumentNullException(nameof(streamWriter));
            var serialized = JSON.Serialize(indexValue);
            streamWriter.WriteLine(serialized);
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
    }
}
