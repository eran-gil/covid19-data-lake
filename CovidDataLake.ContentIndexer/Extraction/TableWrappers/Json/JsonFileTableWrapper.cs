using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;
using Jil;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Json;

class JsonFileTableWrapper : IFileTableWrapper
{
    public JsonFileTableWrapper(string filename)
    {
        Filename = filename;
    }
    public string Filename { get; set; }
    public async Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>> GetColumns()
    {
        var columns = new HashSet<string>();
        using (var fileStream = File.OpenRead(Filename))
        using (var reader = new StreamReader(fileStream))
        {
            while (fileStream.Position < fileStream.Length)
            {
                var line = await reader.ReadLineAsync();
                var currentItem = JSON.Deserialize<Dictionary<string, object>>(line);
                columns.UnionWith(currentItem.Keys);
            }
        }

        return columns.ToDictionary(col => col, GetColumnValues);
    }

    private async IAsyncEnumerable<RawEntry> GetColumnValues(string columnName)
    {
        using var fileStream = File.OpenRead(Filename);
        using var reader = new StreamReader(fileStream);
        while (fileStream.Position < fileStream.Length)
        {
            var line = await reader.ReadLineAsync();
            var currentItem = JSON.Deserialize<Dictionary<string, object>>(line);
            if (currentItem.ContainsKey(columnName))
            {
                yield return new RawEntry(Filename, currentItem[columnName].ToString());
            }
        }
    }
}
