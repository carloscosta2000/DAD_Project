using Google.Protobuf.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeaseManager
{
    internal class Paxos
    {
        private string username;
        private int slot_duration;
        private LeaseRequestsHolder holder;
        private MapField<string, List<string>> requests;
        private Dictionary<string, string> lms;
        private Dictionary<string, string> tms;
        private PaxosConsensusMessages consensus;
        private PaxosInfo info;
        private TimeStepInfo timeStepInfo;

        public Paxos(string username, int index, int slot_duration, Dictionary<string, string> lms, Dictionary<string, string> tms, LeaseManagerClientLogic LMClient_logic, PaxosConsensusMessages paxosGRPC, PaxosInfo info) 
        {
            this.username = username;
            this.slot_duration = slot_duration;
            this.lms = lms;
            this.tms = tms;
            this.holder = LeaseRequestsHolder.Instance;
            this.consensus = paxosGRPC;
            this.info = info;
            this.timeStepInfo = TimeStepInfo.Instance;
            this.startPaxos(index, LMClient_logic);
        }

        public void startPaxos(int index, LeaseManagerClientLogic lMClient_logic) 
        {
            this.requests = createProposal();
            //PAXOS Consensus can start if there are Lease Requests
            if (this.requests.Count > 0)
            {
                
                /*Transforms:
                * - MapField<TM:string, keysToWrite:List<string>>
                * into
                * - MapField<KeytoWrite:string, LeaseOrder>
                *   - LeaseOrder is a repeated string of TMs that want to write on that key.
                */
                MapField<string, LeaseOrder> proposed_leases = new MapField<string, LeaseOrder>();
                //First section: create the data structure.
                foreach (KeyValuePair<string, List<string>> entry in requests)
                    foreach (string key in entry.Value)
                        if (!proposed_leases.ContainsKey(key))
                            proposed_leases[key] = new LeaseOrder();
                //Second section: fill the with the TMs identifiers that want to write on a specific key.
                foreach (string key in proposed_leases.Keys)
                    foreach (string tm_id in tms.Keys)
                        if (requests.ContainsKey(tm_id))
                            foreach (string key_of_tm in requests[tm_id])
                                if (!proposed_leases[key].Tms.Contains(tm_id) && key.Equals(key_of_tm))
                                    proposed_leases[key].Tms.Add(tm_id);

                
                //if (canStartPaxos(proposed_leases) == false)
                    //Console.WriteLine("canStartPaxos line 70");
                    //return;
                //Console.WriteLine(proposed_leases);
                this.consensus.setNewConsensusValue(proposed_leases);

                if (index == (this.timeStepInfo.get_current_timestep() + 1) % tms.Count)
                {
                    Console.WriteLine("\n#####   STARTING PAXOS (LEADER) epoch: " + (info.getEpoch() + 1) + "   #####\n");
                    info.incEpoch();
                    this.holder.clearLeasesRequests();
                    this.consensus.PrepareSend(new PrepareRequest
                    {
                        Id = this.username,
                        ReadTimestamp = consensus.getReadTimestamp(),
                    }, null);
                }
            }
                
        }
        
        private bool canStartPaxos(MapField<string, LeaseOrder> proposed_leases)
        {
            DecidedLeasesHolder holder = DecidedLeasesHolder.Instance;
            MapField<string, LeaseOrder> lastDecided = holder.getLastDecided();
            //If PAXOS never ran, it can start.
            if (lastDecided == null)
                return true;

            int numberOfNonConflictingKeys = 0;
            //Ver as keys que vão ser escritas nesta epoch.
            foreach(KeyValuePair<string, LeaseOrder> entry in proposed_leases)
            {
                //Caso essa key também tenha sido escrita na epoch passada
                if (lastDecided.ContainsKey(entry.Key))
                {
                    LeaseOrder lo_decided = lastDecided[entry.Key];
                    //O último TM que ficou com a lease
                    string tm_lease_holder = lo_decided.Tms[lo_decided.Tms.Count - 1];
                    LeaseOrder lo_proposed = proposed_leases[entry.Key];
                    //Caso agora na proposta essa key continue com o TM que ficou com o lease da mesma na epoch passada 
                    if (lo_proposed.Tms.Count == 1 && lo_proposed.Tms[0].Equals(tm_lease_holder))
                        numberOfNonConflictingKeys++;
                }
            }
            if (numberOfNonConflictingKeys == proposed_leases.Count)
                return false;

            return true;
        }

        private MapField<string, List<string>> createProposal()
        {
            List<LeaseBroadCastRequest> lease_requests = this.holder.getAllLeasesRequests();
            MapField<string, List<string>> requests = new MapField<string, List<string>>();
            foreach (LeaseBroadCastRequest lr in lease_requests)
            {
                if (!requests.ContainsKey(lr.Id))
                {
                    requests.Add(lr.Id, new List<string>());
                    this.holder.leasesToRemoveInNextEpoch.Add(lr);
                }
                foreach (string key in lr.LeaseRequest.Keys.ToArray())
                    if (!requests[lr.Id].Contains(key))
                        requests[lr.Id].Add(key);
            }
            return requests;
        }
    }
}
