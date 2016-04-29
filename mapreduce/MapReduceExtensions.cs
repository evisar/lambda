using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mapreduce
{
    public static class MapReduceExtensions
    {
        public static void Process<TKey, TValue>(this IMapReduce<TKey,TValue> mapreduce, params string[] args)
        {
            var source = args[0];
            var target = args[1];

            try {
                if (args.Length == 2)
                {
                    var map = mapreduce.Map(source);
                    var json = JsonConvert.SerializeObject(map);
                    File.WriteAllText(Path.Combine(target, System.Diagnostics.Process.GetCurrentProcess().Id.ToString()), json);
                }

                if (args.Length == 3)
                {
                    var maps = new List<Dictionary<TKey, TValue>>();
                    foreach (var file in Directory.GetFiles(source))
                    {
                        var map = JsonConvert.DeserializeObject<Dictionary<TKey, TValue>>(File.ReadAllText(file));
                        maps.Add(map);
                        //File.Delete(file);
                    }
                    var result = mapreduce.Reduce(maps);
                    var json = JsonConvert.SerializeObject(result);
                    File.WriteAllText(Path.Combine(target, "out.json"), json);
                }
            }
            catch(Exception ex)
            {
                var sb = new StringBuilder();
                sb.AppendLine(ex.Message);
                sb.AppendLine(ex.StackTrace);
                File.WriteAllText(Path.Combine(target, "error.json"), sb.ToString());
                throw;
            }            
        }

        public static IDictionary<TKey, TValue> Reduce<TKey, TValue>(this IEnumerable<IDictionary<TKey, TValue>> maps, Func<TValue, KeyValuePair<TKey,TValue>, TValue> aggregate)
        {
            var calc = (from map in maps
                        from kv in map
                        group kv by kv.Key into g
                        select new
                        {
                            g.Key,
                            Value = g.Aggregate(default(TValue), aggregate)
                        }).ToDictionary(x => x.Key, x => x.Value);

            return calc;
        }

    }
}
