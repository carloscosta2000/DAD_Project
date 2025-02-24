using System;
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TestServer;

namespace testServer
{
    public enum EnumPaxos
    {
        INIT, PREPARE, ACCEPT
    }

    class Program
    {
        private Dictionary<string, string> all_processes = new Dictionary<string, string>();
        private int process_index;
        private string name;
        private string host_name;
        private int port;
        private PaxosConsensusMessages paxosConsensus;

        public static void Main(string[] args)
        {
            string name = args[0];
            string start_time = args[1];
            string host_name = args[2];
            Program program = new Program(name, start_time, host_name);
            program.mainLoop();
            while (true)
            {
                Thread.Sleep(10000);
            }
        }

        public Program(string name, string start_time, string host)
        {
            this.name = name;
            string[] host_name_splited = host.Split(":");
            this.host_name = host_name_splited[0];// + ":" + host_name_splited[1];
            this.port = Int32.Parse(host_name_splited[1]); //Int32.Parse(host_name_splited[2]);
            Console.WriteLine("http://" + this.host_name + ":" + this.port);
            this.paxosConsensus = new PaxosConsensusMessages(this.name);
            this.startServer();
        }

        private void startServer()
        {
            ServerPort serverPort;
            serverPort = new ServerPort(this.host_name, this.port, ServerCredentials.Insecure);
            Server server = new Server
            {
                Services = { PaxosServices.BindService(this.paxosConsensus) },
                Ports = { serverPort }
            };

            server.Start();
            //Configuring HTTP for client connections in Register method
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        private void startPaxos(DadInt value)
        {
            if((this.name == "TS1" && value.Value == 1800) || ((this.name == "TS2" && value.Value == 1801))) //ver se é o lider ou se é a vez dele de enviar prepare
            {
                Console.WriteLine("Vou começar a ronda do Paxos e sou o " + value);
                try
                {
                    if (!value.Equals(this.paxosConsensus.getAcceptedValue()))
                        this.paxosConsensus.setNewConsensusValue(value);
                    this.paxosConsensus.PrepareSend(new PrepareRequest
                    {
                        Sender = this.name,
                        ReadTimestamp = this.paxosConsensus.getReadTimestamp(),
                    }, null);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        public void mainLoop()
        {
            Thread.Sleep(7000);
            int i = 0;
            while(i<2)
            {
                if (this.paxosConsensus.state == EnumPaxos.INIT)
                {
                    if (i == 0)
                        startPaxos(new DadInt
                        {
                            Key = "teste",
                            Value = 1800,
                        });
                    i++;
                }
                
            }
        }

        public DateTime getStartTime(String line)
        {
            return new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, int.Parse(line.Split(':')[0]), int.Parse(line.Split(':')[1]), int.Parse(line.Split(':')[2]));
        }
        public int getTimeSlot(String line)
        {
            Console.WriteLine(line);
            return int.Parse(line.Split(' ')[1]);
        }

    }

}
