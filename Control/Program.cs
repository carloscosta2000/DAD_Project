namespace control
{
    using System.Collections;
    using System.Diagnostics;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;

    public class Control
    {
        //ArrayList configurations = new ArrayList();

        private int num_timeslots = 0;
        private int time_slot_ms;
        //private String start_time;
        private ArrayList states = new ArrayList();
        private ArrayList processes = new ArrayList();

        public void readConfigFile(String filename)
        {
            try
            {
                string[] lines = File.ReadAllLines(filename);
                foreach (string line in lines)
                    if (!line.StartsWith("#"))
                    {
                        switch (line.ToCharArray()[0])
                        {
                            case 'P':
                                //Console.WriteLine("P command");
                                processes.Add(line);   
                                //configurations.Add(SplitLineIntoArray(line));
                                break;
                            case 'S':
                                //Console.WriteLine("S command");
                                this.num_timeslots =  Int32.Parse(SplitLineIntoArray(line)[0]);
                                //configurations.Add(SplitLineIntoArray(line));
                                break;
                            /*case 'T':
                                //Console.WriteLine("T command");
                                this.start_time = line;
                                break;*/
                            case 'D':
                                //Console.WriteLine("D command");
                                getTimeSlot(line);
                                break;
                            case 'F':
                                //Console.WriteLine("F command");
                                states.Add(line);
                                //configurations.Add(SplitLineIntoArray(line));
                                break;
                            default:
                                break;
                        }
                    }
            }
            catch (FileNotFoundException e) {
                Console.WriteLine("Configuration File is not present!");
                throw e;
            }

            //Check if all processes have different hostnames and ports
            if (!verifyHostnames())
            {
                Console.WriteLine("Error Creating Processes. Make sure that all processes have different hostnames and ports.");
                return;
            }

            execute_processes();
        }

        private bool verifyHostnames()
        {
            List<string> hostnames = new List<string>();
            foreach(string line in processes)
            {
                if (!hostnames.Contains(line.Split(" ")[3]))
                    hostnames.Add(line.Split(" ")[3]);
                else
                    return false;
            }
            return true;
        }

        public String[] SplitLineIntoArray(String line)
        {
            string[] resultArray;
            string[] elements = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (elements.Length > 1)
            {
                resultArray = new string[elements.Length - 1];
                Array.Copy(elements, 1, resultArray, 0, elements.Length - 1);    
                return resultArray;
            }
            else
            {
                Console.WriteLine("Input line contains only one element.");
                return new string[0];
            }
        }

        public void getTimeSlot(String line)
        {
            this.time_slot_ms = int.Parse(line.Split(' ')[1]);
            Console.WriteLine(time_slot_ms);
        }

        public static void Main(string[] args)
        {
            Control config = new Control();
            config.readConfigFile("../../../../configuration.txt");
        }

        private void execute_processes()
        {
            ArrayList lms = new ArrayList();
            ArrayList tms = new ArrayList();

            foreach (String p in this.processes)
            {
                String[] process_info = SplitLineIntoArray(p);
                String type = process_info[1];
                if (!type.Equals("C"))
                {
                    String identifier = process_info[0];
                    String host = process_info[2];
                    lms.Add(identifier + "=" + host);
                }
            }
            int portCounter = 20000;
            DateTime now = DateTime.Now;
            now = now.AddMilliseconds(15000);
            foreach (String p  in this.processes)
            {
                Process process = new Process();
                String[] process_info = SplitLineIntoArray(p);
                String identifier = process_info[0];
                String type = process_info[1];

                String executable_path = "";

                StringBuilder sb = new StringBuilder();
                sb.Append(identifier + " ");
                
                sb.Append(now.ToString("HH:mm:ss") + " ");

                //Transaction Manager
                if (type.Equals("T"))
                {
                    String host = process_info[2];
                    sb.Append(host);
                    executable_path = "../../../../TransactionManager/bin/Debug/net6.0/TransactionManager.exe";
                }
                //Lease Manager
                else if (type.Equals("L"))
                {
                    String host = process_info[2];
                    sb.Append(host);
                    executable_path = "../../../../LeaseManager/bin/Debug/net6.0/LeaseManager.exe";
                }
                //Client
                else
                {
                    portCounter++;
                    sb.Append("localhost:" + portCounter.ToString() + " ");
                    sb.Append(process_info[2]);
                    executable_path = "../../../../Client/bin/Debug/net6.0/Client.exe";
                }
                    
                
                process.StartInfo.FileName = System.IO.Path.Combine(Environment.CurrentDirectory, executable_path);
                process.StartInfo.Arguments = sb.ToString();
                process.StartInfo.UseShellExecute = true;
                process.Start();
            }
        }
    }
}