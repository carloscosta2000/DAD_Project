using Google.Protobuf.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeaseManager
{

    public sealed class PaxosInfo
    {
        private int epoch = 0;

        public void incEpoch() { 
            lock(this) 
                epoch++; 
        }

        public int getEpoch() {
            int epochCopy = 0;
            lock(lockobj)
                epochCopy = this.epoch;
            return epochCopy; 
        }

        PaxosInfo() {}
        private static object lockobj = new object();
        private static PaxosInfo instance;
        public static PaxosInfo Instance
        {
            get
            {
                lock (lockobj)
                {
                    if (instance == null)
                    {
                        instance = new PaxosInfo();
                    }
                    return instance;
                }
            }
        }

    }



        public sealed class DecidedLeasesHolder
    {
        private MapField<string, LeaseOrder> lastDecided;

        public void saveDecided(MapField<string, LeaseOrder> lastDecided)
        {
            this.lastDecided = new MapField<string, LeaseOrder>() { lastDecided };
        }
        public MapField<string, LeaseOrder> getLastDecided() { return this.lastDecided; }

        DecidedLeasesHolder() { }
        private static readonly object lockobj = new object();
        private static DecidedLeasesHolder instance = null;
        public static DecidedLeasesHolder Instance
        {
            get
            {
                lock (lockobj)
                {
                    if (instance == null)
                    {
                        instance = new DecidedLeasesHolder();
                    }
                    return instance;
                }
            }
        }
    }

    public sealed class LeaseRequestsHolder
    {

        private List<LeaseBroadCastRequest> leasesRequests = new List<LeaseBroadCastRequest>();
        private Dictionary<string, Dictionary<string, List<Transactions>>> tmTransactions = new Dictionary<string, Dictionary<string, List<Transactions>>>();
        private static readonly object lockTransactions = new object();
        public List<LeaseBroadCastRequest> leasesToRemoveInNextEpoch = new List<LeaseBroadCastRequest>();

        public void addLeaseRequest(LeaseBroadCastRequest lr) {
            lock (lockobj)
                leasesRequests.Add(lr); 
        }
        public List<LeaseBroadCastRequest> getAllLeasesRequests() {
            lock (lockobj)
                return new List<LeaseBroadCastRequest>(leasesRequests);
        }

        public void clearLeasesRequests() {
            lock (lockobj)
            {
                List<LeaseBroadCastRequest> leasesRequestsCopy = new List<LeaseBroadCastRequest>();
                foreach(LeaseBroadCastRequest lr in leasesRequests)
                {
                    if (!leasesToRemoveInNextEpoch.Contains(lr))
                    {
                        leasesRequestsCopy.Add(lr);
                    }
                }
                this.leasesRequests = new List<LeaseBroadCastRequest>(leasesRequestsCopy);
            }
        }

        public void clearTmTransactions(string lm)
        {
            lock (lockTransactions)
                tmTransactions[lm] = new Dictionary<string, List<Transactions>>();
        }

        public List<Transactions> getTransactionsOfTm(string lm, string tm)
        {
            List<Transactions> copyTransactions = new List<Transactions>();
            lock (lockTransactions)
            {
                if(tmTransactions.ContainsKey(lm) && tmTransactions[lm].ContainsKey(tm))
                    copyTransactions = tmTransactions[lm][tm];
            }
                return copyTransactions;
        }

        public void addTransactionOfTm(string lm, string tm, Transactions transactions)
        {
            lock (lockTransactions)
            {
                if (!tmTransactions.ContainsKey(lm))
                    tmTransactions[lm] = new Dictionary<string, List<Transactions>>();
                if (!tmTransactions[lm].ContainsKey(tm))
                    tmTransactions[lm][tm] = new List<Transactions>();
                tmTransactions[lm][tm].Add(transactions);
            }
        }

        LeaseRequestsHolder() { }
        private static readonly object lockobj = new object ();
        private static LeaseRequestsHolder instance = null;
        public static LeaseRequestsHolder Instance
        {
            get
            {
                lock (lockobj)
                    {
                        if (instance == null)
                        {
                            instance = new LeaseRequestsHolder();
                        }
                        return instance;
                    }
            }
        }
    }
}
