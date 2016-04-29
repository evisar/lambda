using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace lambda.node
{
    class Program
    {
        static void Main(string[] args)
        {
            var dirs = Directory.GetDirectories("c:\\lambda\\cluster\\");

            Console.WriteLine("Running Lambda Cluster with {0} nodes", dirs.Length);

            var iw = new FileSystemWatcher("c:\\lambda\\sessions\\", "*.exe");
            iw.Created += (sender, e) =>
              {
                  Task.Run(() =>
                  {

                      bool failed = true;
                      while (failed)
                          try
                          {
                              using (File.OpenRead(e.FullPath)) { }
                              failed = false;
                          }
                          catch
                          {
                              Task.Delay(333);
                          }


                      var fi = new FileInfo(e.FullPath);

                      var countdown = new System.Threading.CountdownEvent(dirs.Length);
                      var session = Guid.NewGuid().ToString();
                      Console.WriteLine("Session: {0}", session);
                      Console.WriteLine("Timestamp:{0}", DateTime.Now);
                      Console.WriteLine("Process:{0}", e.FullPath);
                      var sw = Stopwatch.StartNew();
                      var di = Directory.CreateDirectory("c:\\lambda\\sessions\\" + session);
                      var pid = new List<int>();
                      var path = e.FullPath;
                      foreach (var dir in dirs)
                      {
                          Process p = Run(path, dir, di.FullName);
                          pid.Add(p.Id);
                          p.EnableRaisingEvents = true;
                          p.Exited += (x, y) => countdown.Signal();
                      }
                      Console.WriteLine("Tasks: {0}", string.Join(",", pid.ToArray()));
                      countdown.Wait();

                      var waiter = new CountdownEvent(1);
                      var pr = Run(path, di.FullName, di.FullName, "1");
                      pr.EnableRaisingEvents = true;
                      pr.Exited += (x, y) => waiter.Signal();
                      sw.Stop();
                      Console.WriteLine("Elapsed: {0}", sw.Elapsed);
                      Console.WriteLine("------------------------------------------");
                      waiter.Wait();

                      File.Delete(e.FullPath);
                  });
              };
            iw.EnableRaisingEvents = true;
            Console.ReadLine();
        }

        private static Process Run(string path, params string[] args)
        {
            var pi = new ProcessStartInfo(path, string.Join(" ", args));
            pi.UseShellExecute = false;
            var p = Process.Start(pi);
            return p;
        }
    }
}
