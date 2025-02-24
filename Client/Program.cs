using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DADTKV;

namespace Client
{
    class Program
    {
        // Name and Host
        private Dictionary<string, string> tms = new Dictionary<string, string>();
        private string name;
        private int num_slots;
        private DateTime start_time;
        private int slot_duration;
        private string path;
        private string host;
        private ArrayList operations;
        private ArrayList readList = new ArrayList();
        private ArrayList writeList = new ArrayList();
        private int current_time_slot;
        private ClientLogic clientLogic;
        private int client_index = 0;
        private List<string> all_tms;
        private List<string> all_lms;

        public Program(string name, string start_time, string host, string configuration_file)
        {
            this.name = name;
            this.path = System.IO.Path.Combine(Environment.CurrentDirectory, "../../../../" + configuration_file);
            this.operations = new ArrayList();
            this.current_time_slot = 1;
            this.start_time = getStartTime(start_time);
            this.host = host;
            this.readConfigFile();
            this.readOperationsFile();
            this.all_tms = this.readTMs();
            this.all_lms = this.readLMs();
            //Console.WriteLine($"{this.client_index} % {tms.Count}=" + (this.client_index % tms.Count));
            //Client index % number of TMs
            KeyValuePair<string, string> pair = tms.ElementAt(this.client_index % tms.Count);
            this.clientLogic = new ClientLogic(this.name, this.host, pair.Key, pair.Value, tms, all_tms, all_lms);
            this.mainLoop();
        }

        public static void Main(string[] args)
        {
            string name = args[0];
            string start_time = args[1];
            string host = args[2];
            string configuration_file = args[3];
            new Program(name, start_time, host, configuration_file);
        }
        public void mainLoop()
        {
            if (operations.Count == 0)
            {
                Console.WriteLine("Client " + this.name + " does not contain operations.");
                return;
            }
            //Wait for syncronization of all processes so that every one starts at the same time.
            DateTime before_operations = DateTime.Now;
            TimeSpan time_span_to_start = start_time.Subtract(before_operations);
            if (time_span_to_start.TotalMilliseconds > 0)
                Thread.Sleep((int)time_span_to_start.TotalMilliseconds);

            while (true)
            {
                foreach (string operation in this.operations)
                {
                    if (operation.StartsWith("T"))
                    {
                        //Send transaction to TM
                        List<string> readList = new List<string>();
                        List<DADTKV.DadInt> writeList = new List<DADTKV.DadInt>();
                        readList = save_read_operations(operation);
                        writeList = save_write_operations(operation);
                        TransactionSubmitReply reply = this.clientLogic.TxSubmit(name, readList, writeList).Result;
                        if (reply == null)
                            Console.WriteLine("Transactions were lost due to Transaction Manager Crashed. \n Connecting to a new Transaction Manager.");
                        else
                        {
                            List<string> readKeysReturned = new List<string>();
                            foreach (DadInt pair in reply.Reads)
                                readKeysReturned.Add(pair.Key);
                            List<string> keysNotFound = readList.Except(readKeysReturned).ToList();
                            foreach (string key in keysNotFound)
                                Console.WriteLine(key + " was null!");
                            foreach (DadInt kvp in reply.Reads.ToArray())
                                Console.WriteLine(kvp.ToString());
                            Console.WriteLine("\n");
                        }
                    }
                    else if (operation.StartsWith("W"))
                    {
                        int wait_time = int.Parse(operation.Split(" ")[1]);
                        Thread.Sleep(wait_time);
                    }
                    else if (operation.StartsWith("S"))
                    {
                        List<StatusReply> reply = clientLogic.sendAllStatus();
                        StringBuilder sb = new StringBuilder();
                        foreach (StatusReply reply_iter in reply)
                        {
                            sb.Append(reply_iter.ToString() + "\n");
                        }
                        Console.WriteLine(sb.ToString());
                    }
                }
            }
        }
        private List<DADTKV.DadInt> save_write_operations(string operation)
        {
            List<DADTKV.DadInt> toReturn = new List<DADTKV.DadInt> ();
            string write_keys_values_raw = operation.Split(" ")[2];
            Dictionary<string, List<int>> intermediate_dict = parse_write_ops(write_keys_values_raw);
            foreach (string key in intermediate_dict.Keys)
            {
                foreach (int value in intermediate_dict[key])
                {
                    DADTKV.DadInt new_dadint = new DADTKV.DadInt();
                    new_dadint.Key = key;
                    new_dadint.Value = value;
                    toReturn.Add(new_dadint);
                }
            }
            return toReturn;
        }
        private Dictionary<string, List<int>> parse_write_ops(string operation)
        {
            Dictionary<string, List<int>> return_value = new Dictionary<string, List<int>>();
            string op_no_parenthesis = remove_parenthesis_from_str(operation);
            string tmp = "";
            Boolean skip_next_comma = false;
            if (op_no_parenthesis.Length != 0)
            {
                foreach (char c in op_no_parenthesis)
                {
                    if (skip_next_comma) {
                        skip_next_comma = false;
                        continue; 
                    }
                    if (c == '<')
                    {
                        tmp = "";
                        continue; 
                    } else if (c == '>')
                    {
                        //End of KV
                        string clean_tmp = clean_string(tmp);
                        if (!return_value.ContainsKey(clean_tmp.Split(",")[0]))
                            return_value[clean_tmp.Split(",")[0]] = new List<int>();
                        return_value[clean_tmp.Split(",")[0]].Add(Int32.Parse(clean_tmp.Split(",")[1]));
                        tmp = "";
                        skip_next_comma = true;
                    } else
                    {
                        tmp += c;
                    }
                }
            }
            return return_value;
        }
        private string remove_parenthesis_from_str(string str)
        {
            var charsToRemove = new string[] { "(", ")" };
            foreach (var c in charsToRemove)
            {
                str = str.Replace(c, "");
            }
            return str;
        }
        private List<string> save_read_operations(string operation)
        {
            List<string> toReturn = new List<string>();
            string read_keys_raw = operation.Split(" ")[1];
            string[] read_keys_splited = read_keys_raw.Split(",");
            if (read_keys_splited.Length == 1)
            {
                string read_key = clean_string(read_keys_splited[0]);
                if (!read_key.Equals(string.Empty))
                    toReturn.Add(read_key);
            }
            else
            {
                foreach (string read_key_raw in read_keys_splited)
                {
                    string read_key = clean_string(read_key_raw);
                    if (!read_key.Equals(string.Empty))
                        toReturn.Add(read_key);
                }
            }
            return toReturn;
        }
        private string clean_string(string str)
        {
            var charsToRemove = new string[] { "(", ")", "<", ">", "\"" };
            foreach (var c in charsToRemove)
            {
                str = str.Replace(c, string.Empty);
            }
            return str;
        }
        private void readOperationsFile()
        {
            try
            {
                string[] lines = File.ReadAllLines(path);
                foreach (string line in lines)
                    if (!line.StartsWith("#"))
                    {
                        this.operations.Add(line);
                    }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Configuration File is not present!");
                throw e;
            }
        }
        private void readConfigFile()
        {
            try
            {
                string[] lines = File.ReadAllLines("../../../../configuration.txt");
                int index = 1;
                foreach (string line in lines)
                    if (!line.StartsWith("#"))
                    {
                        if (line.ToCharArray()[0] == 'P' && line.Split(" ")[2].Equals("T"))
                        {
                            tms.Add(line.Split(" ")[1], line.Split(" ")[3]);
                        }

                        if (line.ToCharArray()[0] == 'P' && line.Split(" ")[2].Equals("C"))
                        {
                            if (line.Split(" ")[1].Equals(this.name))
                                this.client_index = index;
                            else
                                index++;
                        }


                        else if (line.ToCharArray()[0] == 'S')
                            num_slots = Int32.Parse(SplitLineIntoArray(line)[0]);

                        else if (line.ToCharArray()[0] == 'D')
                        {
                            slot_duration = getTimeSlot(line);
                            //Console.WriteLine(line);
                        }
                    }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Configuration File is not present!");
                throw e;
            }
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

        public DateTime getStartTime(String line)
        {
            return new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, int.Parse(line.Split(':')[0]), int.Parse(line.Split(':')[1]), int.Parse(line.Split(':')[2]));
        }
        public int getTimeSlot(String line)
        {
            return int.Parse(line.Split(' ')[1]);
        }

        private List<string> readTMs()
        {
            List<string> toReturn = new List<string>();
            try
            {
                string[] lines = File.ReadAllLines("../../../../configuration.txt");
                foreach (string line in lines)
                    if (!line.StartsWith("#"))
                    {
                        if (line.ToCharArray()[0] == 'P' && line.Split(" ")[2].Equals("T"))
                        {
                            toReturn.Add(line.Split(" ")[3]);
                        }
                    }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Configuration File is not present!");
                throw e;
            }
            return toReturn;
        }

        private List<string> readLMs()
        {
            List<string> toReturn = new List<string>();
            try
            {
                string[] lines = File.ReadAllLines("../../../../configuration.txt");
                foreach (string line in lines)
                    if (!line.StartsWith("#"))
                    {
                        if (line.ToCharArray()[0] == 'P' && line.Split(" ")[2].Equals("L"))
                        {
                            toReturn.Add(line.Split(" ")[3]);
                        }
                    }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Configuration File is not present!");
                throw e;
            }
            return toReturn;
        }

        
    }


}
