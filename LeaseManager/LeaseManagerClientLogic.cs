using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeaseManager
{
    //When the leader decides a value in PAXOS, does a broadcast to all TMs with all the leases decided
    public class LeaseManagerClientLogic
    {

        private Dictionary<string, LeaseAssignService.LeaseAssignServiceClient> tms_channels;
        private string username;
        private string hostname;
        private ProcessesState processesState;
        private TimeStepInfo timeStepInfo;

        public LeaseManagerClientLogic(string username, string hostname, Dictionary<string, string> tms)
        {
            this.username = username;
            this.hostname = hostname;
            this.timeStepInfo = TimeStepInfo.Instance;
            this.processesState = ProcessesState.Instance;
            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            tms_channels = new Dictionary<string, LeaseAssignService.LeaseAssignServiceClient>();
            //Initialize Channels
            foreach (KeyValuePair<string, string> tm in tms)
                tms_channels.Add(tm.Key, new LeaseAssignService.LeaseAssignServiceClient(GrpcChannel.ForAddress("http://" + tm.Value)));
        }

        public void sendDecideLeases(int epoch, MapField<string, LeaseOrder> ordered_leases)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : sendDecideLeases - LeaseManagerClientLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }

            //Console.WriteLine("Sending Decide Leases.");
            LeaseRequestsHolder holder = LeaseRequestsHolder.Instance;
            StringBuilder sb = new StringBuilder();
            sb.Append("Decided Leases Sent epoch: "+ epoch +" time step: "+ timeStepInfo.get_current_timestep() + " \n");
            foreach (KeyValuePair<string, LeaseAssignService.LeaseAssignServiceClient> tm in tms_channels)
            {
                RequestDecidedLeases request = new RequestDecidedLeases();
                request.Id = this.username;
                request.Hostname = tm.Key;
                request.Epoch = epoch;
                foreach (KeyValuePair<string, LeaseOrder> entry in ordered_leases)
                {
                    TransactionManagers list = new TransactionManagers();
                    list.Tms.Add(entry.Value.Tms.ToArray());
                    request.Leases[entry.Key] = list;
                }
                try
                {
                    List<Transactions> transactionsToBeExecutedInEpoch = holder.getTransactionsOfTm(this.username, tm.Key);
                    foreach (Transactions txs in transactionsToBeExecutedInEpoch) 
                    {
                        TransactionsOfEpoch transactionsOfEpoch = new TransactionsOfEpoch();
                        transactionsOfEpoch.Id = tm.Key;
                        foreach(string read in txs.Reads)
                            transactionsOfEpoch.Reads.Add(read);
                        foreach(TransactionDadInt dadints in txs.Writes)
                        {
                            TransactionOfEpochDadInt transactionOfEpochDadInt = new TransactionOfEpochDadInt(); 
                            transactionOfEpochDadInt.Key = dadints.Key;
                            transactionOfEpochDadInt.Value = dadints.Value;
                            transactionsOfEpoch.Writes.Add(transactionOfEpochDadInt);
                        }
                        request.Txs.Add(transactionsOfEpoch);
                    }

                    sb.Append("-> To: " + tm.Key + " Values: " + request.Txs + "\n");
                    
                    tm.Value.DecideLeasesAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    this.processesState.suspectProcess(tm.Key);
                    Console.WriteLine("DecideLeasesAsync timeout. Suspecting process " + tm.Key);
                }

                //TODO verificar o estado do reply
            }
            Console.WriteLine(sb.ToString());
            holder.clearTmTransactions(this.username);
        }
    }
}
