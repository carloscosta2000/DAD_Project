using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeaseManager
{
    public class LRService : LeaseRequestService.LeaseRequestServiceBase
    {
        private string name;
        private string hostname;
        private TimeStepInfo timeStepInfo;
        public LRService(string name, string hostname) {
            this.name = name;
            this.hostname = hostname;
            this.timeStepInfo = TimeStepInfo.Instance;
        }
        
        public override Task<LeaseBroadCastResponse> LeaseBroadCast(
            LeaseBroadCastRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : LeaseBroadCast - LeaseManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }

            LeaseRequestsHolder holder = LeaseRequestsHolder.Instance;
            holder.addLeaseRequest(request);
            holder.addTransactionOfTm(name, request.Id, request.Txs);
            return Task.FromResult(ResponseToLeaseBroadCast(request));
        }

        public LeaseBroadCastResponse ResponseToLeaseBroadCast(LeaseBroadCastRequest request)
        {
            return new LeaseBroadCastResponse
            {
                Id = this.name,
                Hostname = this.hostname,
                Status = true
            };
        }
    }

    public class StatusLMService : StatusService.StatusServiceBase
    {
        private string name;
        private string hostname;
        private int current_time_slot;
        private LeaseRequestsHolder holder;
        HashSet<int> crashed_time_slots = new HashSet<int>();
        private TimeStepInfo timeStepInfo;
        public StatusLMService(string name, string hostname, int current_time_slot, HashSet<int> crashed_time_slots)
        {
            this.name = name;
            this.hostname = hostname;
            this.current_time_slot = current_time_slot;
            this.holder = LeaseRequestsHolder.Instance;
            this.crashed_time_slots = crashed_time_slots;
            this.timeStepInfo = TimeStepInfo.Instance;
        }
        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : Status - LeaseManagerServerLogic");
            }
            List<LeaseBroadCastRequest> all_lease_requests = holder.getAllLeasesRequests();
            string state = "";
            if (crashed_time_slots.Contains(current_time_slot))
                state = "C";
            else
                state = "N";
            PaxosInfo info = PaxosInfo.Instance;
            StatusReply statusReply = new StatusReply
            {
                InstanceId = this.name,
                State = state,
                CurrentEpoch = info.getEpoch(),
                CurrentLeasesRequests = all_lease_requests.Count,
                CurrentTimeSlot = this.current_time_slot,
                Type = "LM"
            };
            //set list of LeaseBroadCastRequest
            foreach (LeaseBroadCastRequest leaseBroadCastRequest in all_lease_requests)
            {
                LeaseRequestStatus lr_status = new LeaseRequestStatus();
                lr_status.Id = leaseBroadCastRequest.Id;
                foreach (string key in leaseBroadCastRequest.LeaseRequest.Keys)
                {
                    lr_status.Keys.Add(key);
                }
                statusReply.LeaseRequests.Add(lr_status);
            }

            //set list of suspected
            foreach (string s in this.timeStepInfo.get_suspected_time_slots()[this.timeStepInfo.get_current_timestep()])
                statusReply.Suspected.Add(s);
            
            
            return Task.FromResult(statusReply);
        }
    }
}
