using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace olap
{
    public interface ICube<TFact, TDimension, TMeasure>
    {
        Func<TFact, TDimension>[] Dimensions { get; }
        Func<TMeasure, TFact, TMeasure>[] Measures { get; }
    }
}
