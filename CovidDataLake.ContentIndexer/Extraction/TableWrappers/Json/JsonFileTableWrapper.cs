using System.Collections.Generic;
using System.Collections.Immutable;
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
    private readonly ImmutableHashSet<StringWrapper> _defaultOriginFileNames;
    private readonly JsonSerializer _serializer;

    private readonly HashSet<JTokenType> AllowedTokenTypes;
    public JsonFileTableWrapper(string filename, string originFilename)
    {
        Filename = filename;
        _originFilename = new StringWrapper(originFilename);
        _defaultOriginFileNames = ImmutableHashSet.Create(_originFilename);
        _serializer = new JsonSerializer();
        JTokenType[] allowedTokenTypes = { JTokenType.Boolean, JTokenType.Date, JTokenType.Float, JTokenType.Guid, JTokenType.Integer, JTokenType.String, JTokenType.Uri };
        AllowedTokenTypes = new HashSet<JTokenType>(allowedTokenTypes);
    }
    public string Filename { get; set; }
    public IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>> GetColumns()
    {
        var items = GetItems();
        var columnWriters = new Dictionary<string, IColumnWriter>();
        foreach (var item in items)
        {
            WriteItemToColumns(item, columnWriters);
        }
        foreach (var columnWriter in columnWriters.Values)
        {
            columnWriter.FinishWriting();
        }

        return columnWriters.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.GetColumnEntries(_defaultOriginFileNames));
    }

    private void WriteItemToColumns(IDictionary<string, string> item, IDictionary<string, IColumnWriter> columnWriters)
    {
        foreach (var (column, value) in item)
        {
            if (!columnWriters.ContainsKey(column))
            {
                columnWriters[column] = new ColumnFileWriter();
            }

            var columnWriter = columnWriters[column];
            columnWriter.WriteValue(value);
        }
    }

    private IEnumerable<IDictionary<string, string>> GetItems()
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        using var jsonReader = new JsonTextReader(reader);
        while (jsonReader.Read())
        {
            if (jsonReader.TokenType != JsonToken.StartObject)
            {
                continue;
            }

            var currentItem = _serializer.Deserialize<Dictionary<string, JToken>>(jsonReader);
            yield return GetPropertiesFromObject(currentItem);
        }
    }

    private IDictionary<string, string> GetPropertiesFromObject(Dictionary<string, JToken> obj)
    {
        var itemDict = new Dictionary<string, string>();
        foreach (var (key, value) in obj)
        {
            if (value == null || !AllowedTokenTypes.Contains(value.Type))
            {
                continue;
            }

            itemDict[key] = value.ToString(Formatting.None);
        }

        return itemDict;
    }
}
