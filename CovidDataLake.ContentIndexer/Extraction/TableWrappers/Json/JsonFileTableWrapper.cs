using System.Collections.Generic;
using System.IO;
using System.Linq;
using CovidDataLake.ContentIndexer.Extraction.Models;
using Jil;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Json;

class JsonFileTableWrapper : IFileTableWrapper
{
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly StringWrapper _originFilename;
    private readonly List<StringWrapper> _defaultOriginFilenames;

    public JsonFileTableWrapper(string filename, string originFilename)
    {
        Filename = filename;
        _originFilename = new StringWrapper(originFilename);
        _defaultOriginFilenames = new List<StringWrapper>{_originFilename};
    }
    public string Filename { get; set; }
    public IEnumerable<KeyValuePair<string, IEnumerable<RawEntry>>> GetColumns()
    {
        var columns = new HashSet<string>();
        using (var fileStream = File.OpenRead(Filename))
        using (var reader = new StreamReader(fileStream))
        {
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var currentItem = JSON.Deserialize<Dictionary<string, object>>(line);
                columns.UnionWith(currentItem.Keys);
            }
        }

        return columns.ToDictionary(col => col, GetColumnValues);
    }

    private IEnumerable<RawEntry> GetColumnValues(string columnName)
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        var values = new HashSet<string>();
        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            var currentItem = JSON.Deserialize<Dictionary<string, object>>(line);
            if (!currentItem.ContainsKey(columnName))
            {
                continue;
            }

            var value = currentItem[columnName].ToString();
            if (values.Contains(value))
            {
                continue;
            }

            values.Add(value);
            yield return new RawEntry(_defaultOriginFilenames, value);
        }
    }
}
