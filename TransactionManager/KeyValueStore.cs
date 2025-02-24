using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;
using Grpc.Net.Client;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using System.Runtime.CompilerServices;
using System.Xml;
using System.Timers;
using System.Threading;
using System.Diagnostics.Metrics;

namespace TransactionManager
{
    public sealed class TransactionsManager
    {
        private static readonly object lockobj = new object();
        private static readonly object checkQuorumLock = new object();
        private static readonly object keyValuesLock = new object();
        private static readonly object waitingListLock = new object();
        private static readonly object stateRequestLock = new object();
        private static readonly object heldLeasesLock = new object();
        private int lockWrite;

        private static TransactionsManager? instance;
        private Dictionary<string, DADTKV.DadInt> keyvalues;
        private Dictionary<string, List<string>> decided_leases;
        private Dictionary<string, List<string>> waiting_list; //<key, list<tm_to_write_before>>
        private Dictionary<string, System.Timers.Timer> timers;
        private int lastEpochDone;
        private Dictionary<int, List<RequestDecidedLeases>> checkQuorumDict;
        private List<StateRequest> checkQuorumStateRequest;
        private List<string> LMsList;
        private Dictionary<String, string> tmList;
        private RepService replicationGRPC;
        private ProcessesState processesState;
        private string hostname;
        private RequestDecidedLeases requestDecidedLeases;
        private static readonly int INTERVAL_TIMER = 1000;
        private List<string> heldLeases;

        private TransactionsManager(string username)
        {
            this.keyvalues = new Dictionary<string, DADTKV.DadInt>();
            this.decided_leases = new Dictionary<string, List<string>>();
            this.lastEpochDone = -1;
            this.checkQuorumDict = new Dictionary<int, List<RequestDecidedLeases>>();
            this.checkQuorumStateRequest = new List<StateRequest>();
            this.LMsList = getLMsList();
            this.tmList = getTMs();
            this.processesState = ProcessesState.Instance;
            this.processesState.initDataStructure(tmList.Keys.ToList());
            this.replicationGRPC = new RepService(username, tmList);
            this.hostname = username;
            this.waiting_list = new Dictionary<string, List<string>>();
            this.requestDecidedLeases = new RequestDecidedLeases();
            this.timers = new Dictionary<string, System.Timers.Timer>();
            this.heldLeases = new List<string>();
            this.lockWrite = 0;
        }

        public static TransactionsManager Instance(string username)
        {
            lock (lockobj)
            {
                if (instance == null)
                {
                    instance = new TransactionsManager(username);
                }
                return instance;
            }
        }

        private Dictionary<string, List<string>> convertRequestToDict(RequestDecidedLeases request)
        {
            Dictionary<string, List<string>> newDict = new Dictionary<string, List<string>>();
            Dictionary<string, TransactionManagers> leases = request.Leases.ToDictionary(kv => kv.Key, kv => kv.Value);
            foreach (string key in leases.Keys)
            {
                newDict[key] = new List<string>();
                foreach (string tm in leases[key].Tms)
                    newDict[key].Add(tm);
            }
            return newDict;
        }

        /**
         * This functions executes when a TM receives a RequestDecidedLeases from an LM
         * The TM will add to dictionary the requests of a given a epoch
         * Once the number of requests reach an quorum, the TM is ready to proceed with the Operations
         * Before it proceeds with the Operations it will update the decided_leases, the lastEpochDone, the held_Leases and the waiting_list
         */
        public void decideLeases(RequestDecidedLeases request)
        {
            lock (checkQuorumLock)
            {
                if (!this.checkQuorumDict.ContainsKey(request.Epoch) && request.Epoch > lastEpochDone)
                {
                    this.checkQuorumDict[request.Epoch] = new List<RequestDecidedLeases>();
                    this.decided_leases = convertRequestToDict(request);
                }
                if (this.checkQuorumDict.ContainsKey(request.Epoch))
                {
                    checkQuorumDict[request.Epoch].Add(request);
                    if (this.checkQuorumDict[request.Epoch].Count >= ((LMsList.Count / 2) + 1))
                    {
                        this.lastEpochDone = request.Epoch;
                        this.checkQuorumDict.Remove(request.Epoch);
                        this.requestDecidedLeases = request;
                        
                        StringBuilder sb = new StringBuilder();
                        sb.Append("\n#####   ! QUORUM OF DECIDES !   #####\n");
                        sb.Append("- EPOCH :     " + request.Epoch + "\n");
                        sb.Append("- Transactions : " + request.Txs + "\n");
                        sb.Append("- Leases :    " + MyDictionaryToJson(this.decided_leases) + "\n");
                        sb.Append("\n");
                        Console.WriteLine(sb.ToString());
                        lock (waitingListLock)
                        {
                            foreach (string key in this.decided_leases.Keys)
                            {
                                this.waiting_list[key] = new List<string>();
                                if (this.decided_leases[key].Contains(this.hostname)){
                                    foreach (string value in this.decided_leases[key])
                                    {
                                        if (!value.Equals(this.hostname))
                                            this.waiting_list[key].Add(value); //<key, list<tm_to_write_before>>
                                        else
                                        {
                                            this.waiting_list[key].Add(value); //myself
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        foreach (string key in decided_leases.Keys)
                            createTimer(key, null, false);
                        lock (heldLeasesLock)
                        {
                            foreach(string key in decided_leases.Keys)
                            {
                                if(this.heldLeases.Contains(key)) //no longer helds this key
                                    this.heldLeases.Remove(key);
                                if (decided_leases[key].Last().Equals(this.hostname))
                                    this.heldLeases.Add(key);
                            }
                        }
                        proceedOperations();
                    }
                }

            }
        }

        /**
         * Checks if there is any TM left in the waiting list
         */
        private int countWaiting()
        {
            int counter = 0;
            lock(waitingListLock)
            {
                foreach (string key in waiting_list.Keys)
                {
                    if (this.waiting_list[key].Count > 0)
                        counter += this.waiting_list[key].Count;
                }
            }
            return counter;
        }

        /**
         * Loop that checks if the TM has the leases to write in any key
         * If it does it executes the write operations 
         */
        public void proceedOperations()
        {
            if(countWaiting() > 0)
            {
                List<string> keyList = CheckIfCanWrite();
                if (keyList.Count == 0)
                {
                    //If am not the first in any key
                    //Create a thread that waits for the message of replication from the 
                    //TM that is writing before its my turn, also create a timeout for the possibility
                    //that the TM that is writing, either because of suspicions or falts, doesnt send the message
                    //of replication
                    checkWaitingList();
                }
                else
                {
                    lockWrite = 1;
                    foreach (string key in keyList)
                        writeInKey(key);
                }
            }
        }

        /**
         * Checks the Waiting List with a lock
         * If it's only waiting for the las 
         */
        private void checkWaitingList()
        {
            lock (waitingListLock)
                foreach (string key in this.waiting_list.Keys)
                    if (this.waiting_list[key].Count == 2)
                        createTimer(key, this.waiting_list[key].First(), true);
        }

        
        //Creates a timer either active or inactive depending on the enabled value
        private void createTimer(string key, string tm, bool enabled)
        {
            this.timers[key] = new System.Timers.Timer();
            this.timers[key].Elapsed += (sender, e) => OnTimedEvent(key, tm);
            this.timers[key].Interval = INTERVAL_TIMER;
            this.timers[key].Enabled = enabled;
        }

        /**
         * Function that is triggered when a timeout occurs
         * A TM that was waiting for a TM to write and replicate, didn't receive that replication
         * the timeout occurs and it will broadcast RecoverRequest for that key
         * before starting the write ops on that key
         */
        private void OnTimedEvent(string key, string tm)
        {
            if (this.waiting_list[key].Contains(tm))
            {
                this.timers[key].Stop();
                this.timers[key].Close();
                Console.WriteLine("$$$ RECOVER OF KEY: " + key + " $$$");
                RecoverRequest request = new RecoverRequest();
                request.Tm = this.hostname;
                request.Key = key;
                this.checkQuorumStateRequest = new List<StateRequest>();
                this.replicationGRPC.SendRecoverRequest(request, null);
            }
            if (this.timers.ContainsKey(key) && !this.timers[key].Equals(null))
            {
                this.timers[key].Stop();
                this.timers[key].Close();
            }
        }

        /**
         * Checks if this TM can write in any key
         * If it can add that key to a list so it executes the write operations over those keys
        */
        private List<string> CheckIfCanWrite()
        {
            List<string> keyList = new List<string>();
            lock (waitingListLock)
                foreach (string key in this.waiting_list.Keys)
                    if (this.waiting_list[key].Count > 0 && this.waiting_list[key].First().Equals(this.hostname))
                        keyList.Add(key);
            return keyList;
        }

        
        //It holds the lease to write on that key, therefore it executes the write operations on that key
        private void writeInKey(string key)
        {
            if(lockWrite == 1)
            {
                lockWrite = 0;
                List<TransactionsOfEpoch> txs = this.requestDecidedLeases.Txs.ToList();
                foreach (TransactionsOfEpoch transaction in txs)
                    if (transaction.Id.Equals(this.requestDecidedLeases.Hostname))
                    {
                        List<TransactionOfEpochDadInt> writes = transaction.Writes.ToList();
                        StringBuilder sb1 = new StringBuilder();
                        sb1.Append("- Replicated keys: ");
                        StringBuilder sb = new StringBuilder();
                        sb.Append("\n#####   WRITING in epoch: " + this.requestDecidedLeases.Epoch + "   #####\n");
                        sb.Append("- Written keys: ");
                        foreach (TransactionOfEpochDadInt writeOp in writes)
                            if (writeOp.Key.Equals(key))
                            {
                                sb.Append("<" + key + "," + writeOp.Value + ">; ");
                                sb1.Append(writeAndReplicate(this.requestDecidedLeases, writeOp.Key, writeOp.Value, sb1));
                            }
                       
                        sb.Append("\n" + sb1.ToString() + "\n");
                        Console.WriteLine(sb.ToString() + "\n");
                    }
            }
        }

        /**
         * Writes a value in a given key between epochs because it still holds the lease
         * of that key, then replicates via broadcast to the other TMs the changes.
         */
        public bool writeInKeyWithoutLeases(string key, int value)
        {
            Console.WriteLine("\n#####   WRITING (Already has Lease of key: "+ key +") <" + key + "," + value + "> in epoch " + (this.lastEpochDone + 1)+ "  #####\n");
            this.keyvalues[key] = new DADTKV.DadInt { Key = key, Value= value};
            DadIntReplicate dadIntReplicate = new DadIntReplicate { Key = key, Value = value };
            ReplicateRequest replicateRequest = new ReplicateRequest { Tm = this.hostname, Epoch = (this.lastEpochDone + 1), KeyValue = dadIntReplicate, HeldLeases = true };
            Task<ReplicateReply> reply = this.replicationGRPC.SendReplicate(replicateRequest, null);
            return reply.Result.Ok;
        }

        /**
         * Writes a value on a given key in the KeyValueStore and replicates via 
         * broadcast to the other TMs the changes
        */
        private string writeAndReplicate(RequestDecidedLeases request, string key, int value, StringBuilder sb)
        {
            changeValue(key, value);
            removeTMWaiting(key, this.hostname);
            DadIntReplicate dadIntReplicate = new DadIntReplicate { Key = key, Value = value };
            ReplicateRequest replicateRequest = new ReplicateRequest { Tm = request.Hostname, Epoch = request.Epoch, KeyValue = dadIntReplicate, HeldLeases = false};
            Task<ReplicateReply> reply = this.replicationGRPC.SendReplicate(replicateRequest, null);
            sb.Append("<" + request.Hostname + ", " + dadIntReplicate.Key + ">, ");
            return sb.ToString();
        }

        /**
         * When a TM receives a ReplicateRequest it updates the KeyValueStore
         * Removes TMs that are already wrote from the Waiting list
         * And removes a timer if there is one
         * Finally proceeds with the Write operations if its his turn and if it has any left 
         */
        public void updateState(ReplicateRequest request)
        {
            if(request.HeldLeases) {
                Console.WriteLine("#####   WRITING REPLICATION (Previously Attributed Lease) from  " + request.Tm + " with " + request.KeyValue.ToString() + "   #####");
            }else
                Console.WriteLine("#####   WRITING REPLICATION from " + request.Tm + " with " +request.KeyValue.ToString()+ "   #####");
            changeValue(request.KeyValue.Key, request.KeyValue.Value);
            removeTMWaiting(request.KeyValue.Key, request.Tm); //Verificações de epoch???
            removeTimer(request.KeyValue.Key);
            if (!request.HeldLeases)
            {
                proceedOperations();
            }
        }

        /**
         * When a TM receives a RecoverRequest, prepares a StateRequest that will carry the value of that key
         * a sends the request to the TM that is recovering the state of that key
        */
        public void receiveRecoverRequest(RecoverRequest request)
        {
            StateRequest stateRequest = new StateRequest();
            stateRequest.Sender = this.hostname;
            stateRequest.Receiver = request.Tm;
            lock (keyValuesLock)
                if(this.keyvalues.ContainsKey(request.Key))
                    stateRequest.KeyValue = new DadIntSate
                    {
                        Key = request.Key,
                        Value = this.keyvalues[request.Key].Value
                    };
            Console.WriteLine("RECOVER SEND TO " + stateRequest.Receiver + " KEY - " + stateRequest.KeyValue.Key);
            this.replicationGRPC.SendStateRequest(stateRequest, null);
        }

        /**
        * When a TM is waiting for the lease to write in a given key, and the timer 
        * expires the TM begins a state update via broadcast to the other TM's
        * to update its value of that key before writing in that key
        */
        public void receiveStateRequest(StateRequest request)
        {
            Console.WriteLine("STATE RECEIVED FROM " + request.Sender + " VALUE -> " + request.KeyValue);
            lock (stateRequestLock)
            {
                this.checkQuorumStateRequest.Add(request);
                if(this.checkQuorumStateRequest.Count + 1 >= ((tmList.Count / 2) + 1))
                {
                    Console.WriteLine("QUORUM STATE RECOVER - " + request.KeyValue.ToString());
                    changeValue(request.KeyValue.Key, request.KeyValue.Value);
                    removeTMWaiting(request.KeyValue.Key, this.waiting_list[request.KeyValue.Key].First());
                    this.checkQuorumStateRequest = new List<StateRequest>();
                    proceedOperations();
                }
            }
        }

        /**
         * Updates the value associated to that key with a lock
         */
        private void changeValue(string key, int value)
        {
            lock (keyValuesLock)
            {
                this.lockWrite = 0;
                keyvalues[key] = new DADTKV.DadInt { Key = key, Value = value };
            }
        }

        /**
         * Removes the TM that already wrote ok that key from the waiting list
         */
        private void removeTMWaiting(string key, string tm)
        {
            lock (waitingListLock)
                if (this.waiting_list.ContainsKey(key) && this.waiting_list[key].Contains(tm))
                    this.waiting_list[key].Remove(tm);
        }

        /**
         * Stop and Closes the timer for that key
         */
        private void removeTimer(string key)
        {
            lock (waitingListLock)
                if (this.timers.ContainsKey(key) && !this.timers[key].Equals(null) && this.timers[key].Enabled)
                {
                    this.timers[key].Stop();
                    this.timers[key].Close();
                }
        }

        private string MyDictionaryToJson(Dictionary<string, List<string>> dict)
        {
            var entries = dict.Select(d =>
                string.Format("\"{0}\": [{1}]", d.Key, string.Join(",", d.Value)));
            return "{" + string.Join(",", entries) + "}";
        }

        private string DictToString(Dictionary<string, string> dict)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var pair in dict)
            {
                sb.Append($"{pair.Key}: {pair.Value}");
            }
            return sb.ToString();
        }

        private static List<string> getLMsList()
        {
            List<string> toReturn = new List<string>();
            string[] lines = File.ReadAllLines("../../../../configuration.txt");
            foreach (string line in lines)
            {
                if (!line.StartsWith("#"))
                {
                    string[] elements = Regex.Split(line, @"\s+"); // Split using regular expression to handle variable spaces.
                    if (elements.Length >= 3 && elements[2] == "L")
                    {
                        toReturn.Add(line);
                    }
                }
            }
            return toReturn;
        }

        private static Dictionary<string, string> getTMs()
        {
            Dictionary<string, string> toReturn = new Dictionary<string, string>();
            string[] lines = File.ReadAllLines("../../../../configuration.txt");
            foreach (string line in lines)
            {
                if (!line.StartsWith("#"))
                {
                    string[] elements = Regex.Split(line, @"\s+"); // Split using regular expression to handle variable spaces.
                    if (elements.Length >= 3 && elements[2] == "T")
                    {
                        toReturn.Add(elements[1], elements[3]);
                    }
                }
            }  
            return toReturn;
        }

        /**
         * Reads the value of the given keys with a lock
         */
        public List<DADTKV.DadInt> getReads(string[] keys)
        {
            List<DADTKV.DadInt> toReturn = new List<DADTKV.DadInt>();
            foreach (string key in keys)
                lock (keyValuesLock)
                    if (keyvalues.ContainsKey(key))
                        toReturn.Add(keyvalues[key]);
            return toReturn;
        }

        //getter for the last Epoch done 
        public int getLastEpochDone()
        {
            return this.lastEpochDone;
        }

        //getter for the decided_leases
        public Dictionary<string, List<string>> getDecidedLeases()
        {
            return this.decided_leases;
        }

        /**
         * Gets the current Held Leases
         */
        public List<string> getHeldLeases()
        {
            List<string> heldLeasesCopy;
            lock (heldLeasesLock)
                heldLeasesCopy = new List<string>(this.heldLeases);
            return heldLeasesCopy;
        }

        /**
         * Retrieves the state of this TM with a lock
         */
        public List<DADTKV.DadInt> getState()
        {
            List < DADTKV.DadInt > state = new List<DADTKV.DadInt> ();
            lock (keyValuesLock)
                foreach(string key in keyvalues.Keys)
                    state.Add(keyvalues[key]);
            return state;   
        }
        /**
         * Updates the state of this TM with the UpdateRequest obtained from a quorum of TM's 
         */
        public void recoverState(UpdateStateRequest request)
        {
            lock (keyValuesLock)
                foreach (DadIntRecover dadInt in request.State)
                {
                    if (!keyvalues.ContainsKey(dadInt.Key))
                        
                        keyvalues.Add(dadInt.Key, new DADTKV.DadInt { Key = dadInt.Key, Value = dadInt.Value });
                    else 
                        keyvalues[dadInt.Key] = new DADTKV.DadInt { Key = dadInt.Key, Value = dadInt.Value };
                }
            Console.WriteLine("Returned and Now I'm updated!!"); 
        }

    }
}
