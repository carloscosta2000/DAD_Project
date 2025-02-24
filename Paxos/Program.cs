using System.Collections;
using System.Diagnostics;
using System.Text;

namespace paxos
{
    public class Program
    {

        public static void Main(string[] args)
        {
            Program program = new Program();
            ArrayList processes = new ArrayList
                {
                    "TS1",
                    "TS2",
                    "TS3",
                    "TS4",
                    "TS5"
                };
            program.execute_processes(processes);
        }

        private void execute_processes(ArrayList processes)
        {
            int i = 0;
            foreach (String p in processes)
            {
                Process process = new Process();
                String identifier = p;
                int host_port = 10000 + i;
                String host = "localhost:" + host_port.ToString();
                i++;
                String executable_path = "";

                StringBuilder sb = new StringBuilder();
                sb.Append(identifier + " ");
                DateTime now = DateTime.Now;
                now.AddMilliseconds(4000);
                sb.Append(now.ToString("HH:mm:ss") + " ");
                sb.Append(host);

                executable_path = "../../../../TestServer/bin/Debug/net6.0/TestServer.exe";

                process.StartInfo.FileName = System.IO.Path.Combine(Environment.CurrentDirectory, executable_path);
                process.StartInfo.Arguments = sb.ToString(); // if you need some
                process.StartInfo.UseShellExecute = true;
                process.StartInfo.CreateNoWindow = true;
                process.Start();
            }
        }
    }
}

