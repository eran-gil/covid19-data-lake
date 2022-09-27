using System;
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
        try
        {
            var columns = GetColumnNames();
            return columns.ToDictionary(col => col, GetColumnValues);
        }
        catch (Exception ex)
        {
            return Enumerable.Empty<KeyValuePair<string, IEnumerable<RawEntry>>>();
        }

    }

    private IEnumerable<string> GetColumnNames()
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        var columns = new HashSet<string>();
        var firstChar = (char)reader.Read();
        reader.DiscardBufferedData();
        fileStream.Seek(0, SeekOrigin.Begin);
        var items = firstChar == '[' ? GetItemsByArray(reader) : GetItemsByRow(reader);

        foreach (var item in items)
        {
            columns.UnionWith(item.Keys);
        }

        return columns;
    }

    private static IEnumerable<Dictionary<string, object>> GetItemsByArray(StreamReader reader)
    {
        return JSON.Deserialize<List<Dictionary<string, object>>>(reader.ReadToEnd());
    }

    private static IEnumerable<Dictionary<string, object>> GetItemsByRow(StreamReader reader)
    {
        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            var currentItem = JSON.Deserialize<Dictionary<string, object>>(line);
            yield return currentItem;
        }
    }

    private IEnumerable<RawEntry> GetColumnValues(string columnName)
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        var values = new HashSet<string>();
        var firstChar = (char)reader.Read();
        reader.DiscardBufferedData();
        fileStream.Seek(0, SeekOrigin.Begin);
        var items = firstChar == '[' ? GetItemsByArray(reader) : GetItemsByRow(reader);
        foreach (var item in items)
        {
            if (!item.ContainsKey(columnName))
            {
                continue;
            }

            var value = item[columnName].ToString();
            if (values.Contains(value))
            {
                continue;
            }

            values.Add(value);
            yield return new RawEntry(_defaultOriginFilenames, value);
        }
    }
}
