using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mapreduce
{
    public interface IMapReduce<TKey, TValue>
    {
        IDictionary<TKey, TValue> Map(params object[] args);
        IDictionary<TKey,TValue> Reduce(IEnumerable<IDictionary<TKey, TValue>> maps);
    }
}
