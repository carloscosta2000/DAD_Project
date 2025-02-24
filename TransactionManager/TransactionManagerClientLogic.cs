using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionManager
{
    public class TransactionManagerClientLogic
    {
        private Dictionary<string, LeaseRequestService.LeaseRequestServiceClient> lms_channels;
        private string username;
        private string hostname;
        private TimeStepInfo timeStepInfo;
        public TransactionManagerClientLogic(string username, string hostname, Dictionary<string, string> lms)
        {
            this.username = username;
            this.hostname = hostname;
            this.timeStepInfo = TimeStepInfo.Instance;
            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            lms_channels = new Dictionary<string, LeaseRequestService.LeaseRequestServiceClient>();
            //Initialize Channels
            foreach (KeyValuePair<string, string> lm in lms)
            {
                lms_channels.Add(lm.Key, new LeaseRequestService.LeaseRequestServiceClient(GrpcChannel.ForAddress("http://" + lm.Value)));
            }
        }

        public void sendLeaseBroadCast(List<string> keys_to_write, Transactions transaction_to_LM)
        {

            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : sendLeaseBroadCast - TransactionManagerClientLogic");
                return;
            }

            LeaseRequest leaseRequest = new LeaseRequest();
            foreach (string key in keys_to_write)
                leaseRequest.Keys.Add(key);

            foreach (KeyValuePair<string, LeaseRequestService.LeaseRequestServiceClient> lm in lms_channels)
            {
                lm.Value.LeaseBroadCastAsync(new LeaseBroadCastRequest
                {
                    Id = this.username,
                    Hostname = this.hostname,
                    LeaseRequest = leaseRequest,
                    Txs = transaction_to_LM

                }, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))); //TODO Lidar com a exeception
            }
            //TODO agurdar confirmaçao de write de keys para permitir a funçao TxSubmit ler as keys do request e responder ao cliente.
            //Esperar confirmaçao de todos que não sejam suspected?
        }
    }
}
