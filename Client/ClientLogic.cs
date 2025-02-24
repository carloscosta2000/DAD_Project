using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Grpc.Core;
using Grpc.Net.Client;

namespace Client
{
    public class ClientLogic
    {
        private GrpcChannel channel;
        private TransactionService.TransactionServiceClient server;
        private Dictionary<string, string> tms = new Dictionary<string, string>();
       
        private int port;
        private string host;
        private string tm_name;
        private string tm_host;
        private string username;
        private List<GrpcChannel> all_hosts_channels;
        private List<StatusService.StatusServiceClient> all_hosts_servers;
        public readonly int DEADLINE = 15;
        //list of suspects
        private Dictionary<string, bool> states;

        public ClientLogic(string username, string clientHostname, string tm_name, string tm_host, Dictionary<string, string> tms, List<string> all_tm_hosts, List<string> all_lm_hosts) 
        {
            string[] values = clientHostname.Split(":");
            this.port = Int32.Parse(values[1]);
            this.host = values[0];
            this.tm_host = tm_host;
            this.tm_name = tm_name;
            this.username = username;
            this.tms = tms; // Name and Host
            this.all_hosts_channels = new List<GrpcChannel>();
            this.all_hosts_servers = new List<StatusService.StatusServiceClient>();

        AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            Console.WriteLine("$$$ IDENTIFICATION: " + this.username + " $$$");
            Console.WriteLine("\nConnected to: " + this.tm_name + " at -> http://" + tm_host+"\n");
            channel = GrpcChannel.ForAddress("http://" + tm_host);
            server = new TransactionService.TransactionServiceClient(channel);
            //TM's
            foreach (string host in all_tm_hosts)
            {
                all_hosts_channels.Add(GrpcChannel.ForAddress("http://" + host));
            }
            //LM's
            foreach (string host in all_lm_hosts)
            {
                all_hosts_channels.Add(GrpcChannel.ForAddress("http://" + host));
            }
            foreach (GrpcChannel channel in all_hosts_channels)
            {
                all_hosts_servers.Add(new StatusService.StatusServiceClient(channel));
            }
            initDataStructure();
        }

        public async Task<TransactionSubmitReply> TxSubmit(string client_name, List<string> read_keys, List<DADTKV.DadInt> write_keys) {
            Console.WriteLine("Sending Transaction to " + this.tm_name);
            TransactionSubmitRequest request = new TransactionSubmitRequest();
            request.Id = client_name;

            string[] read_keysS = (string[]) read_keys.ToArray();
            request.Reads.Add(read_keysS);

            //foreach(string r in read_keysS)
                //Console.WriteLine(r);

            foreach (DADTKV.DadInt write_key in write_keys)
            {
                DadInt mewDadInt = new DadInt();
                mewDadInt.Key = write_key.Key;
                mewDadInt.Value = write_key.Value;
                request.Writes.Add(mewDadInt);
            }
            TransactionSubmitReply reply = new TransactionSubmitReply();
            try
            {
                reply = server.TxSubmit(request, deadline: DateTime.UtcNow.AddSeconds(DEADLINE));
                Console.WriteLine("Received Reply from " + this.tm_name);
                return reply;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
            {
                Console.WriteLine("TxSubmit ClientLogic: " + ex.StatusCode + "\n" + ex.Message);
                suspectProcess(this.tm_name);
                reconnectToAnotherTM();
            }
            return reply;
        }

        private void initDataStructure()
        {
            states = new Dictionary<string, bool>();
            foreach (string id in this.tms.Keys)
                states[id] = false;
        }

        private void suspectProcess(string id)
        {
            if (states != null && states.ContainsKey(id))
                states[id] = true;
        }

        private void stopSuspectingProcess(string id)
        {
            if (states != null && states.ContainsKey(id))
                states[id] = false;
        }

        private void reconnectToAnotherTM()
        {
            //bool isConnected = false;
            Random random = new Random();
            Dictionary<string, string> tms_without_crashed = new Dictionary<string, string>(this.tms);
            tms_without_crashed.Remove(this.tm_name);
            int size = tms_without_crashed.Count;
            int index = random.Next(0, size);
            string name = tms_without_crashed.ElementAt(index).Key;
            this.tm_name = name;
            this.tm_host = tms_without_crashed[name];
            this.channel = GrpcChannel.ForAddress("http://" + this.tm_host);
            this.server = new TransactionService.TransactionServiceClient(channel);
            Console.WriteLine("Reconnected to : " + tm_name);
        }

        public List<StatusReply> sendAllStatus()
        {
            Console.WriteLine("Sending Status Request to " + tm_name);
            List<StatusReply> toReturn = new List<StatusReply>();
            foreach (StatusService.StatusServiceClient server in all_hosts_servers)
            {
                toReturn.Add(server.Status(new StatusRequest { Id = this.host }));
            }
            return toReturn;
        }





       
    }
}
