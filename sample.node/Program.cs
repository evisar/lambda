using mapreduce;
using Newtonsoft.Json;
using olap;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sample.node
{

    public class Order
    {
        public int PurchaseOrderID;
        public int PurchaseOrderDetailID;
        public DateTime DueDate;
        public decimal OrderQty;
        public int ProductID;
        public decimal UnitPrice;
        public decimal LineTotal;
    }

    public class OrderCube : ICube<Order, int, decimal>
    {
        public Func<Order, int>[] Dimensions { get; } = new Func<Order, int>[] 
        {
            x=> x.DueDate.Year,
            x=> x.DueDate.Month,
            x=> x.DueDate.Day
        };

        public Func<decimal, Order, decimal>[] Measures { get; } = new Func<decimal, Order, decimal>[]
        {
            (x,y) => x + y.LineTotal
        };
    }

    public class FileTypeCalculator : IMapReduce<string, int>
    {
        public IDictionary<string, int> Map(object[] args)
        {
            var source = args[0].ToString();

            var calc = (from file in Directory.GetFiles(source)
                       group file by Path.GetExtension(file) into g
                       select new
                       {
                           Key = g.Key,
                           Value = g.Count()
                       }).ToDictionary(x=> x.Key, x=> x.Value);

            return calc;            
        }

        public IDictionary<string,int> Reduce(IEnumerable<IDictionary<string, int>> maps)
        {
            return maps.Reduce((x, y) => x + y.Value);
        }
    }
    public class FileSizeCalculator : IMapReduce<string, long>
    {
        public IDictionary<string, long> Map(object[] args)
        {
            var source = args[0].ToString();

            var calc = (from file in Directory.GetFiles(source)
                        group file by Path.GetExtension(file) into g
                        select new
                        {
                            Key = g.Key,
                            Value = g.Sum(x => new FileInfo(x).Length)
                        }).ToDictionary(x => x.Key, x => x.Value);

            return calc;
        }

        public IDictionary<string, long> Reduce(IEnumerable<IDictionary<string, long>> maps)
        {
            return maps.Reduce((x, y) => x + y.Value);
        }
    }
    public class SumOfOrdersByProductId : IMapReduce<int, decimal>
    {
        public IDictionary<int, decimal> Map(params object[] args)
        {
            var source = args[0].ToString();

            var dic = new Dictionary<int, decimal>();
            foreach(var file in Directory.GetFiles(source, "*.json"))
            {
                var json = File.ReadAllText(file);
                var orders = JsonConvert.DeserializeObject<Order[]>(json);

                foreach(var order in orders)
                {
                    if(!dic.ContainsKey(order.ProductID))
                    {
                        dic[order.ProductID] = order.LineTotal;
                    }
                    else
                    {
                        dic[order.ProductID] = dic[order.ProductID] + order.LineTotal;
                    }
                }
            }
            return dic;
        }

        public IDictionary<int, decimal> Reduce(IEnumerable<IDictionary<int, decimal>> maps)
        {
            return maps.Reduce((x,y)=> x + y.Value);
        }
    }
    public class SumOfOrdersByYearAndProductId : IMapReduce<string, decimal>
    {
        public IDictionary<string, decimal> Map(params object[] args)
        {
            var source = args[0].ToString();

            var dic = new Dictionary<string, decimal>();
            foreach (var file in Directory.GetFiles(source, "*.json"))
            {
                var json = File.ReadAllText(file);
                var orders = JsonConvert.DeserializeObject<Order[]>(json);

                foreach (var order in orders)
                {
                    var tuple = string.Join(",", new []{ order.DueDate.Year, order.ProductID });
                    if (!dic.ContainsKey(tuple))
                    {
                        dic[tuple] = order.LineTotal;
                    }
                    else
                    {
                        dic[tuple] = dic[tuple] + order.LineTotal;
                    }
                }
            }
            return dic;
        }

        public IDictionary<string, decimal> Reduce(IEnumerable<IDictionary<string, decimal>> maps)
        {
            return maps.Reduce((x, y) => x + y.Value);
        }
    }

    public class OrdersOlap : IMapReduce<string, decimal[]>
    {
        public IDictionary<string, decimal[]> Map(params object[] args)
        {
            var source = args[0].ToString();
            var cube = new OrderCube();

            var dic = new Dictionary<string, decimal[]>();
            foreach (var file in Directory.GetFiles(source, "*.json"))
            {
                var json = File.ReadAllText(file);
                var orders = JsonConvert.DeserializeObject<Order[]>(json);

                foreach (var order in orders)
                {
                    var tuple = string.Join(",", cube.Dimensions.Select(x=> x(order)));
                    decimal[] values;
                    if(!dic.TryGetValue(tuple, out values))
                    {
                        values = new decimal[cube.Measures.Length];
                    }
                    for(int i=0;i<cube.Measures.Length;++i)
                    {
                        values = cube.Measures.Select(x => x(values[i], order)).ToArray();
                    }
                    dic[tuple] = values;
                }
            }
            return dic;
        }

        public IDictionary<string, decimal[]> Reduce(IEnumerable<IDictionary<string, decimal[]>> maps)
        {
            return maps.Reduce((x, y) =>  (x ?? new decimal[y.Value.Length]).Zip(y.Value, (a,b)=> a+b).ToArray());
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            new OrdersOlap().Process(args);
            //new SumOfOrdersByYearAndProductId().Process(args);
            //new SumOfOrdersByProductId().Process(args);
            //new FileSizeCalculator().Process(args);
            //new OrdersOlap().Process(args);
            //File.WriteAllText(Process.GetCurrentProcess().Id.ToString(), "0");
        }
    }
}
