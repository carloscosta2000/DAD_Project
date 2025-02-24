using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace LeaseManager
{
    public enum EnumPaxos
    {
        INIT, PREPARE, ACCEPT
    }
    public class PaxosConsensusMessages : PaxosServices.PaxosServicesBase
    {
        private readonly object promiseLock;
        private ArrayList promiseArray;
        private readonly object acceptedArrayLock;
        private ArrayList acceptedArray;
        private readonly object acceptedValueLock;
        private MapField<string, LeaseOrder> accepted_value;
        private readonly object writeTimestampLock;
        private int write_timestamp;
        private readonly object readTimestampLock;
        private int read_timestamp;
        public EnumPaxos state;
        private Dictionary<String, PaxosServices.PaxosServicesClient> nodesList;
        private String host;
        private Boolean isUpdated;
        public LeaseManagerClientLogic LMClient_logic;
        private ProcessesState processesState;
        private MapField<string, LeaseOrder> new_consensus_value;
        private PaxosInfo paxosInfo;
        private LeaseRequestsHolder holder;
        private TimeStepInfo timeStepInfo;
        public PaxosConsensusMessages(LeaseManagerClientLogic LMClient_logic, String host, Dictionary<string, string> lms)
        {
            this.processesState = ProcessesState.Instance;
            this.processesState.initDataStructure(lms);
            this.promiseLock = new object();
            this.acceptedArrayLock = new object();
            this.acceptedValueLock = new object();
            this.writeTimestampLock = new object();
            this.readTimestampLock = new object();
            this.write_timestamp = -1;
            this.read_timestamp = -1;
            this.state = EnumPaxos.INIT;
            this.host = host;
            this.isUpdated = true;
            this.LMClient_logic = LMClient_logic;
            this.accepted_value = new MapField<string, LeaseOrder>();
            this.new_consensus_value = new MapField<string, LeaseOrder>();
            this.paxosInfo = PaxosInfo.Instance;
            this.nodesList = new Dictionary<string, PaxosServices.PaxosServicesClient>();
            this.timeStepInfo = TimeStepInfo.Instance;

            foreach (KeyValuePair<string, string> entry in lms)
                this.nodesList.Add(entry.Key, new PaxosServices.PaxosServicesClient(GrpcChannel.ForAddress("http://" + entry.Value)));

            this.promiseArray = new ArrayList();
            this.acceptedArray = new ArrayList();
            this.holder = LeaseRequestsHolder.Instance;
        }

        public void set_proposal(MapField<string, LeaseOrder> proposed_value) { this.accepted_value = proposed_value; }

        public int getReadTimestamp()
        {
            int get;
            lock (this.readTimestampLock)
            {
                get = this.read_timestamp;
            }
            return get;
        }

        public MapField<string, LeaseOrder> getAcceptedValue()
        {
            MapField<string, LeaseOrder> get;
            lock (this.acceptedValueLock)
            {
                get = this.accepted_value;
            }
            return get;
        }

        public void setNewConsensusValue(MapField<string, LeaseOrder> value)
        {
            this.new_consensus_value = new MapField<string, LeaseOrder>();
            foreach (KeyValuePair<string, LeaseOrder> entry in value)
                this.new_consensus_value.Add(entry.Key, entry.Value);
        }

        public override Task<PrepareReply> PrepareSend(PrepareRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : PrepareSend - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendPrepare(request));
        }

        private PrepareReply sendPrepare(PrepareRequest request)
        {
            if (state.Equals(EnumPaxos.INIT))
            {
                lock (readTimestampLock)
                {
                    this.read_timestamp++;
                }
                PrepareRequest prepareRequest = new PrepareRequest
                {
                    ReadTimestamp = this.read_timestamp,
                    Id = this.host
                };
                this.state = EnumPaxos.PREPARE;
                foreach (String p in nodesList.Keys)
                {
                    if (!p.Equals(this.host) && !this.timeStepInfo.isSuspect(p))
                    {
                        try
                        {
                            this.nodesList[p].PrepareReceiveAsync(prepareRequest, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                        }
                        catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                        {
                            this.processesState.suspectProcess(p);
                            Console.WriteLine("PrepareReceiveAsync timeout. Suspecting process " + p);
                        }   
                    }
                }
            }

            return new PrepareReply
            {
                Ok = true,
            };
        }

        public override Task<PrepareReply> PrepareReceive(PrepareRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : PrepareReceive - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receivePrepare(request));
        }

        private PrepareReply receivePrepare(PrepareRequest request)
        {
            
            this.holder.clearLeasesRequests();

            if (this.state.Equals(EnumPaxos.INIT))
                this.state = EnumPaxos.PREPARE;
            if (request.ReadTimestamp > this.read_timestamp)
            {
                //Console.WriteLine("ReadTimestamp maior que a minha -> válido!");
                lock (readTimestampLock)
                {
                    this.read_timestamp = request.ReadTimestamp;
                }
                PromiseRequest promiseRequest = new PromiseRequest();
                promiseRequest.Id = this.host;
                promiseRequest.Receiver = request.Id;
                lock (writeTimestampLock)
                {
                    promiseRequest.WriteTimestamp = write_timestamp;
                }
                lock (acceptedValueLock)
                {
                    promiseRequest.AcceptedValue.Clear();
                    foreach (KeyValuePair<string, LeaseOrder> entry in accepted_value)
                        promiseRequest.AcceptedValue.Add(entry.Key, entry.Value);
                }
                this.paxosInfo.incEpoch();
                this.nodesList[this.host].PromiseSendAsync(promiseRequest, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5)));
                Console.WriteLine("\n#####   RECEIVED Prepare epoch: " + this.paxosInfo.getEpoch() + " from leader:" + request.Id + "   #####\n");
            }
            else
            {
                Console.WriteLine("ReadTimestamp menor que a minha -> Inválido!!!!!");
            }
            return new PrepareReply
            {
                Ok = true,
            };
        }

        public override Task<PromiseReply> PromiseSend(PromiseRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : PromiseSend - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendPromise(request));

        }

        private PromiseReply sendPromise(PromiseRequest request)
        {
            if (!this.timeStepInfo.isSuspect(request.Receiver))
            {
                try
                {
                    //Console.WriteLine("Enviando Promises para " + request.Receiver);
                    this.nodesList[request.Receiver].PromiseReceiveAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    this.processesState.suspectProcess(request.Receiver);
                    Console.WriteLine("PromiseReceiveAsync timeout. Suspecting process " + request.Receiver);
                }
            }
            return new PromiseReply
            {
                Status = true,
            };
        }

        public override Task<PromiseReply> PromiseReceive(PromiseRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : PromiseReceive - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receivePromise(request));

        }

        private AcceptRequest formatAcceptRequest(MapField<string, LeaseOrder> value)
        {
            this.state = EnumPaxos.ACCEPT;
            AcceptRequest acceptRequest = new AcceptRequest();
            acceptRequest.Id = host;
            acceptRequest.ReadTimestamp = this.read_timestamp;
            acceptRequest.AcceptedValue.Clear();
            foreach (KeyValuePair<string, LeaseOrder> entry in value)
                acceptRequest.AcceptedValue.Add(entry.Key, entry.Value);
            return acceptRequest;
        }

        private PromiseReply receivePromise(PromiseRequest request)
        {

            //Console.WriteLine("Recebi Promise\n  - " + request.Id + "\n  - " + request.WriteTimestamp + "\n  - " + request.AcceptedValue);
            bool hasQuorum = false;
            lock (this)
            {
                if (this.state.Equals(EnumPaxos.PREPARE))
                {
                    this.promiseArray.Add(request);
                    hasQuorum = evaluatePromises();
                }
                //Console.WriteLine(this.promiseArray.Count);
            }
            if (hasQuorum)
            {
                this.state = EnumPaxos.ACCEPT;
                AcceptRequest acceptRequest = formatAcceptRequest(accepted_value);
                //TODO lidar com a exeception
                this.nodesList[this.host].AcceptSendAsync(acceptRequest, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5)));
            }
            return new PromiseReply
            {
                Status = true,
            };
        }

        private bool evaluatePromises()
        {
            if (this.promiseArray.Count + 1 >= ((nodesList.Count / 2) + 1))
            {
                //Console.WriteLine("Existe Quorúm de Promises!!");
                foreach (PromiseRequest r in this.promiseArray)
                    lock (readTimestampLock)
                        if (r.WriteTimestamp > this.read_timestamp)
                        {
                            //Console.WriteLine("Lider estava atrasado! Atualizando os valores!");
                            this.read_timestamp = r.WriteTimestamp;
                            lock (writeTimestampLock)
                                this.write_timestamp = r.WriteTimestamp;
                            lock (acceptedValueLock)
                            {
                                this.accepted_value = r.AcceptedValue;
                                this.isUpdated = false;
                            }
                        }
                if (this.isUpdated)
                    this.accepted_value = this.new_consensus_value;
                return true;
            }
            return false;
        }

        public override Task<AcceptReply> AcceptSend(AcceptRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : AcceptSend - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendAccept(request));
        }

        private AcceptReply sendAccept(AcceptRequest request)
        {
            //Console.WriteLine("Sending Accepts...");
            foreach (String p in nodesList.Keys)
            {
                if (!p.Equals(this.host) && !this.timeStepInfo.isSuspect(p))
                {
                    try
                    {
                        this.nodesList[p].AcceptReceiveAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                    }
                    catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                    {
                        this.processesState.suspectProcess(p);
                        Console.WriteLine("AcceptReceiveAsync timeout. Suspecting process " + p);
                    }
                }
            }
            return new AcceptReply
            {
                Status = true,
            };
        }

        public override Task<AcceptReply> AcceptReceive(AcceptRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : AcceptReceive - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receiveAccept(request));
        }

        private AcceptedRequest formatAcceptedRequest(AcceptRequest request)
        {
            this.state = EnumPaxos.INIT;
            AcceptedRequest acceptedRequest = new AcceptedRequest();
            acceptedRequest.Sender = this.host;
            acceptedRequest.Receiver = request.Id;
            acceptedRequest.WriteTimestamp = this.write_timestamp;
            acceptedRequest.AcceptedValue.Clear();
            foreach (KeyValuePair<string, LeaseOrder> entry in this.accepted_value)
                acceptedRequest.AcceptedValue.Add(entry.Key, entry.Value);
            return acceptedRequest;
        }

        private AcceptReply receiveAccept(AcceptRequest request)
        {
            //Console.WriteLine("Recebi Accept " + paxosInfo.getEpoch() + "\n");//  - " + request.Id + "\n  - " + request.ReadTimestamp + "\n  - " + request.AcceptedValue);
            lock (this.readTimestampLock)
            {
                this.read_timestamp = request.ReadTimestamp;
            }
            lock (writeTimestampLock)
            {
                this.write_timestamp = request.ReadTimestamp;
            }
            lock (acceptedValueLock)
            {
                this.accepted_value = request.AcceptedValue;
            }
            AcceptedRequest acceptedRequest = formatAcceptedRequest(request);
            this.nodesList[this.host].AcceptedSendAsync(acceptedRequest);
            DecidedLeasesHolder holder = DecidedLeasesHolder.Instance;
            holder.saveDecided(this.accepted_value);
            this.promiseArray = new ArrayList();
            this.acceptedArray = new ArrayList();
            this.state = EnumPaxos.INIT;
            LMClient_logic.sendDecideLeases(this.paxosInfo.getEpoch(), this.accepted_value);
            return new AcceptReply
            {
                Status = true,
            };
        }

        public override Task<AcceptedReply> AcceptedSend(AcceptedRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : AcceptedSend - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendAccepted(request));
        }

        private AcceptedReply sendAccepted(AcceptedRequest request)
        {
            //Console.WriteLine("Enviar Accepted para " + request.Receiver);
            if (!this.timeStepInfo.isSuspect(request.Receiver))
            {
                try
                {
                    this.nodesList[request.Receiver].AcceptedReceiveAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    this.processesState.suspectProcess(request.Receiver);
                    Console.WriteLine("AcceptedReceiveAsync timeout. Suspecting process " + request.Receiver);
                }
            }
            return new AcceptedReply
            {
                Status = true,
            };
        }

        public override Task<AcceptedReply> AcceptedReceive(AcceptedRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : AcceptedReceive - PaxosConsensusLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receiveAccepted(request));
        }

        private AcceptedReply receiveAccepted(AcceptedRequest request)
        {
            //Console.WriteLine("Recebi Accepted\n  - " + request.Sender + "\n  - " + request.WriteTimestamp + "\n  - " + request.AcceptedValue);
            lock (acceptedArrayLock)
            {
                if (this.state.Equals(EnumPaxos.ACCEPT))
                {
                    this.acceptedArray.Add(request);
                    evaluateAccepteds();
                }
                //Console.WriteLine(this.acceptedArray.Count);
            }
            return new AcceptedReply
            {
                Status = true,
            };
        }

        private void evaluateAccepteds()
        {
            if (this.acceptedArray.Count + 1 >= ((nodesList.Count / 2) + 1))
                decide();
        }

        private void decide()
        {
            Console.WriteLine("\n#####   DECIDE epoch: " + this.paxosInfo.getEpoch() + "   #####\n" + this.accepted_value);
            this.promiseArray = new ArrayList();
            this.acceptedArray = new ArrayList();
            this.state = EnumPaxos.INIT;
            this.isUpdated = true;
            DecidedLeasesHolder holder = DecidedLeasesHolder.Instance;
            holder.saveDecided(this.accepted_value);
            LMClient_logic.sendDecideLeases(this.paxosInfo.getEpoch(), this.accepted_value);
        }

    }
}
