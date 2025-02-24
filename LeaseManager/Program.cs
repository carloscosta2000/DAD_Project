using Grpc.Core;
using System.Collections;
using System.Text;

namespace LeaseManager
{
    class Program
    {
        private Dictionary<string, string> tms = new Dictionary<string, string>();
        private Dictionary<string, string> lms = new Dictionary<string, string>();
        private Dictionary<string, string> all_processes = new Dictionary<string, string>();
        private int process_index;
        private string name;
        private string host_name;
        private int port;
        private int num_slots;
        private DateTime start_time;
        private int slot_duration;
        private int current_time_slot;
        private HashSet<int> crashed_time_slots = new HashSet<int>();
        private Dictionary<int, ArrayList> suspected_by_in_timeslot = new Dictionary<int, ArrayList>();
        private LeaseManagerClientLogic LMClient_logic;
        private Server server;
        private int lm_index;
        private PaxosConsensusMessages paxosGRPC;
        private ProcessesState processesState;
        private TimeStepInfo timeStepInfo;
        public Program(string name, string start_time, string host)
        {
            this.name = name;
            string[] host_name_splited = host.Split(":");
            this.host_name = host_name_splited[0];// + ":" + host_name_splited[1];
            this.port = Int32.Parse(host_name_splited[1]); //Int32.Parse(host_name_splited[2]);
            this.start_time = getStartTime(start_time);
            this.current_time_slot = 0;
            this.readConfigFile();
            this.timeStepInfo = TimeStepInfo.Instance;
            this.timeStepInfo.set_crashed_time_slots(this.crashed_time_slots);
            this.timeStepInfo.set_suspected_time_slots(this.suspected_by_in_timeslot);
            this.LMClient_logic = new LeaseManagerClientLogic(this.name, this.host_name + ":" + this.port, tms);
            this.paxosGRPC = new PaxosConsensusMessages(this.LMClient_logic, this.name, this.lms);
            this.processesState = ProcessesState.Instance;
            this.startServer();
            this.start_time_stepping();
            
        }

        public static void Main(string[] args)
        {
            string name = args[0];
            string start_time = args[1];
            string host = args[2];
            new Program(name, start_time, host);
            Console.ReadLine();
        }

        public void start_time_stepping()
        {
            Console.WriteLine("$$$ IDENTIFICATION: " + name + " $$$");
            Console.WriteLine("$$$ ADDRESS: " + this.host_name + ":" + this.port + " $$$");
            Console.WriteLine(slot_duration);
            //Wait for syncronization of all processes so that every one starts at the same time.
            DateTime before_operations = DateTime.Now;
            TimeSpan time_span_to_start = start_time.Subtract(before_operations);
            if (time_span_to_start.TotalMilliseconds > 0)
                Thread.Sleep((int)time_span_to_start.TotalMilliseconds);

            Thread checkTimeStepThread = new Thread(() =>
            {
                bool hasFinished = false;
                LeaseRequestsHolder holder = LeaseRequestsHolder.Instance;
                do
                {
                    this.timeStepInfo.increment_timestep();
                    if (timeStepInfo.get_current_timestep() > num_slots)
                    {
                        hasFinished = true;
                        break;
                    }
                    Thread.Sleep(slot_duration);
                    
                    //Reset the States of Each LM in each time step
                    processesState.initDataStructure(this.lms);

                    lock (this)
                    {
                        //Console.WriteLine("current_time_slot = " + current_time_slot);
                        Paxos paxos = new Paxos(this.name, this.lm_index, this.slot_duration, this.lms, this.tms, this.LMClient_logic, this.paxosGRPC, PaxosInfo.Instance);
                        //Console.WriteLine(DateTime.Now.ToString("HH:mm:ss"));
                    }
                } while (!hasFinished);

            });
            checkTimeStepThread.Start();
        }

        private void startServer()
        {
            ServerPort serverPort;
            serverPort = new ServerPort(this.host_name, this.port, ServerCredentials.Insecure);
            try
            {
                this.server = new Server
                {
                    Services = { LeaseRequestService.BindService(new LRService(this.name, this.host_name + ":" + this.port)), 
                        PaxosServices.BindService(this.paxosGRPC), 
                        StatusService.BindService(new StatusLMService(this.name, this.host_name + ":" + this.port, this.current_time_slot, crashed_time_slots)) },
                    Ports = { serverPort }
                };
                this.server.Start();
                //Configuring HTTP for client connections in Register method
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                //TODO terminar o servidor (fazer também no TransactionManager
                //server.KillAsync().Wait();
            }
            catch (IOException e)
            {
                Console.WriteLine("Could not open the server on " + this.host_name + ":" + this.port);
            }
        }
        private void readConfigFile()
        {
            try
            {
                ArrayList crashed_indexes = new ArrayList();
                string[] lines = File.ReadAllLines("../../../../configuration.txt");
                int index = 0;
                foreach (string line in lines)
                    if (!line.StartsWith("#"))
                    {
                        if (line.ToCharArray()[0] == 'P' && line.Split(" ")[2].Equals("T"))
                        {
                            tms.Add(line.Split(" ")[1], line.Split(" ")[3]);
                            //All processes must be saved because the Crashed or Normal states are set by index
                            all_processes.Add(line.Split(" ")[1], line.Split(" ")[3]);
                                
                        }
                        else if ((line.ToCharArray()[0] == 'P' && line.Split(" ")[2].Equals("L")))
                        {
                            lms.Add(line.Split(" ")[1], line.Split(" ")[3]);
                            all_processes.Add(line.Split(" ")[1], line.Split(" ")[3]);
                            //Index used to determine the leader of PAXOS
                            if (line.Split(" ")[1].Equals(this.name))
                            {
                                this.process_index = all_processes.Count - 1;
                                this.lm_index = index;
                            }
                                  
                            index++;
                        }

                        else if (line.ToCharArray()[0] == 'S')
                            num_slots = Int32.Parse(SplitLineIntoArray(line)[0]);
                        else if (line.ToCharArray()[0] == 'D')
                            slot_duration = getTimeSlot(line);
                        else if (line.ToCharArray()[0] == 'F')
                        {
                            List<string> f_command_splitted = line.Split(" ").ToList();
                            List<string> all_states = f_command_splitted.GetRange(2, all_processes.Count);
                            List<string> suspectedList = f_command_splitted.GetRange(2 + all_processes.Count, f_command_splitted.Count - 2 - all_processes.Count);
                            int f_time_step = int.Parse(f_command_splitted[1]);

                            if (all_states.Count < all_processes.Count)
                            {
                                Console.WriteLine("The number os processes described in the F command is not equal to the number of spawned processes.");
                                //return;
                            }

                            if (all_states[process_index].Equals("C"))
                                crashed_time_slots.Add(f_time_step);

                            if (!this.suspected_by_in_timeslot.ContainsKey(f_time_step))
                                this.suspected_by_in_timeslot[f_time_step] = new ArrayList();

                            //if there is suspected list
                            if (suspectedList[0].Length > 2)
                                foreach (string suspicion in suspectedList)
                                {
                                    string[] clean_suspicion = this.clean_string(suspicion).Split(",");
                                    if (clean_suspicion[0].Equals(this.name))
                                    {
                                        suspected_by_in_timeslot[f_time_step].Add(clean_suspicion[1]);
                                    }
                                }
                        }
                    }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Configuration File is not present!");
                throw e;
            }
        }
        public DateTime getStartTime(String line)
        {
            return new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, int.Parse(line.Split(':')[0]), int.Parse(line.Split(':')[1]), int.Parse(line.Split(':')[2]));
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
        public int getTimeSlot(String line)
        {
            return int.Parse(line.Split(' ')[1]);
        }
        private string clean_string(string str)
        {
            var charsToRemove = new string[] { "(", ")" };
            foreach (var c in charsToRemove)
            {
                str = str.Replace(c, string.Empty);
            }
            return str;
        }
    }
}