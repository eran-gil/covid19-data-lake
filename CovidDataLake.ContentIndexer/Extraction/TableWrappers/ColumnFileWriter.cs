using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    internal class ColumnFileWriter : BaseColumnWriter, IDisposable
    {
        private readonly FileStream _file;
        private readonly StreamWriter _writer;

        public ColumnFileWriter()
        {
            var filename = Path.Join(CommonKeys.TEMP_FOLDER_NAME, $"column_{Guid.NewGuid()}.txt");
            _file = File.Open(filename, FileMode.Create, FileAccess.ReadWrite);
            _writer = new StreamWriter(_file, Encoding.UTF8, -1, true);
        }

        public override void WriteValue(string value)
        {
            if (!ShouldWriteValue(value))
            {
                return;
            }
            _writer.WriteLine(value);
            AddDistinctValue(value);
        }

        public override async Task WriteValueAsync(string value)
        {
            if (!ShouldWriteValue(value))
            {
                return;
            }
            await _writer.WriteLineAsync(value);
            AddDistinctValue(value);
        }

        public override IAsyncEnumerable<RawEntry> GetColumnEntries(List<StringWrapper> originFileNames)
        {
            var rawValues = GetValues();
            return GetFilteredEntries(rawValues.ToAsyncEnumerable(), originFileNames);
        }

        public override void FinishWriting()
        {
            _writer?.Dispose();
            _file.Seek(0, SeekOrigin.Begin);
            base.FinishWriting();
        }

        private IEnumerable<string> GetValues()
        {
            using var reader = new StreamReader(_file);
            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine();
            }
        }

        public void Dispose()
        {
            _writer?.Dispose();
            _file?.Dispose();
        }
    }
}
