using System.Collections.Generic;
using System.IO;
using System.Linq;
using CovidDataLake.ContentIndexer.Extraction.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Json;

class JsonFileTableWrapper : IFileTableWrapper
{
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly StringWrapper _originFilename;
    private readonly List<StringWrapper> _defaultOriginFilenames;
    private readonly JsonSerializer _serializer;

    public JsonFileTableWrapper(string filename, string originFilename)
    {
        Filename = filename;
        _originFilename = new StringWrapper(originFilename);
        _defaultOriginFilenames = new List<StringWrapper>{_originFilename};
        _serializer = new JsonSerializer();
    }
    public string Filename { get; set; }
    public IEnumerable<KeyValuePair<string, IEnumerable<RawEntry>>> GetColumns()
    {
        try
        {
            var columns = GetColumnNames();
            return columns.ToDictionary(col => col, GetColumnValues);
        }
        catch
        {
            return Enumerable.Empty<KeyValuePair<string, IEnumerable<RawEntry>>>();
        }

    }

    private IEnumerable<string> GetColumnNames()
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        using var jsonReader = new JsonTextReader(reader);
        var columns = new HashSet<string>();
        var items = GetItems(jsonReader);

        foreach (var item in items)
        {
            columns.UnionWith(item.Keys);
        }

        return columns;
    }

    private IEnumerable<Dictionary<string, string>> GetItems(JsonReader reader)
    {
        while (reader.Read())
        {
            if (reader.TokenType != JsonToken.StartObject)
            {
                continue;
            }

            var currentItem = _serializer.Deserialize<JObject>(reader);
            Dictionary<string, string> convertedItem;
            try
            {
                convertedItem = currentItem?.ToObject<Dictionary<string, string>>();
            }
            catch
            {
                continue;
            }

            if (convertedItem != null)
            {
                yield return convertedItem;
            }

        }
    }

    private IEnumerable<RawEntry> GetColumnValues(string columnName)
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        using var jsonReader = new JsonTextReader(reader);
        var values = new HashSet<string>();
        var items = GetItems(jsonReader);
        foreach (var item in items)
        {
            if (!item.ContainsKey(columnName))
            {
                continue;
            }

            var value = item[columnName];
            if (value == null || values.Contains(value))
            {
                continue;
            }

            values.Add(value);
            yield return new RawEntry(_defaultOriginFilenames, value);
        }
    }
}
