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
using testServer;

namespace TestServer
{


    public class PaxosConsensusMessages : PaxosServices.PaxosServicesBase
    {
        private readonly object promiseLock;
        private ArrayList promiseArray;
        private readonly object acceptedArrayLock;
        private ArrayList acceptedArray;
        private readonly object acceptedValueLock;
        private DadInt accepted_value;
        private readonly object writeTimestampLock;
        private int write_timestamp;
        private readonly object readTimestampLock;
        private int read_timestamp;
        public EnumPaxos state;
        private Dictionary<String, PaxosServices.PaxosServicesClient> nodesList;
        private String host;
        private Boolean isUpdated;

        private DadInt new_consensus_value;
        public PaxosConsensusMessages(String host)
        {
            this.promiseLock = new object();
            this.acceptedArrayLock = new object();
            this.acceptedValueLock = new object();
            this.writeTimestampLock = new object();
            this.readTimestampLock = new object();
            this.write_timestamp = -1;
            this.read_timestamp = -1;
            this.accepted_value = new DadInt
            {
                Key = "default",
            };
            this.new_consensus_value = null;
            this.state = EnumPaxos.INIT;
            this.host = host;
            this.isUpdated = true;
            this.nodesList = new Dictionary<string, PaxosServices.PaxosServicesClient>(){
                {"TS1", new PaxosServices.PaxosServicesClient(GrpcChannel.ForAddress("http://" +"localhost:10000"))},
                {"TS2", new PaxosServices.PaxosServicesClient(GrpcChannel.ForAddress("http://" +"localhost:10001"))},
                {"TS3", new PaxosServices.PaxosServicesClient(GrpcChannel.ForAddress("http://" +"localhost:10002"))},
                {"TS4", new PaxosServices.PaxosServicesClient(GrpcChannel.ForAddress("http://" +"localhost:10003"))},
                {"TS5", new PaxosServices.PaxosServicesClient(GrpcChannel.ForAddress("http://" +"localhost:10004"))}
            };
            this.promiseArray = new ArrayList();
            this.acceptedArray = new ArrayList();
        }

        public int getReadTimestamp()
        {
            int get;
            lock (this.readTimestampLock)
            {
                get = this.read_timestamp;
            }
            return get;
        }

        public DadInt getAcceptedValue()
        {
            DadInt get;
            lock (this.acceptedValueLock)
            {
                get = this.accepted_value;
            }
            return get;
        }

        public void setNewConsensusValue(DadInt value)
        {
            this.new_consensus_value = value;   
        }

        public override Task<PrepareReply> PrepareSend(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(sendPrepare(request));
        }

        private PrepareReply sendPrepare(PrepareRequest request)
        {
            Console.WriteLine("Sou " + request.Sender +" -> Enviando Prepares...");
            if (state.Equals(EnumPaxos.INIT))
            {
                lock (readTimestampLock)
                {
                    this.read_timestamp++;
                }
                PrepareRequest prepareRequest = new PrepareRequest
                {
                    ReadTimestamp = this.read_timestamp,
                    Sender = this.host
                };
                this.state = EnumPaxos.PREPARE;
                foreach (String p in nodesList.Keys)
                {
                    if(!p.Equals(this.host))
                        this.nodesList[p].PrepareReceive(prepareRequest);
                }
            }
            
            return new PrepareReply
            {
                Ok = true,
            };
        }

        public override Task<PrepareReply> PrepareReceive(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(receivePrepare(request));
        }

        private PrepareReply receivePrepare(PrepareRequest request)
        {
            Console.WriteLine("Recebi Prepare do " + request.Sender + " : " + request.ReadTimestamp);   
            if (this.state.Equals(EnumPaxos.INIT))
                this.state = EnumPaxos.PREPARE;
            if (request.ReadTimestamp > this.read_timestamp)
            {
                Console.WriteLine("ReadTimestamp maior que a minha -> válido!");
                lock (readTimestampLock)
                {
                    this.read_timestamp = request.ReadTimestamp;
                }
                PromiseRequest promiseRequest = new PromiseRequest();
                promiseRequest.Sender = this.host;
                promiseRequest.Receiver = request.Sender;
                lock (writeTimestampLock)
                {
                    promiseRequest.WriteTimestamp = write_timestamp;
                }
                lock (acceptedValueLock)
                {
                    promiseRequest.AcceptedValue = accepted_value;
                }
                this.nodesList[this.host].PromiseSend(promiseRequest);
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
            return Task.FromResult(sendPromise(request));

        }

        private PromiseReply sendPromise(PromiseRequest request)
        {
            Console.WriteLine("Enviando Promises para " + request.Receiver);
            this.nodesList[request.Receiver].PromiseReceive(request);
            return new PromiseReply
            {
                Ok = true,
            };
        }

        public override Task<PromiseReply> PromiseReceive(PromiseRequest request, ServerCallContext context)
        {
            return Task.FromResult(receivePromise(request));

        }

        private PromiseReply receivePromise(PromiseRequest request)
        {
            Console.WriteLine("Recebi Promise\n  - " + request.Sender + "\n  - " + request.WriteTimestamp + "\n  - " + request.AcceptedValue);
            bool hasQuorum = false;
            lock (this)
            {
                if (this.state.Equals(EnumPaxos.PREPARE))
                {
                    this.promiseArray.Add(request);
                    hasQuorum = evaluatePromises();
                }
                Console.WriteLine(this.promiseArray.Count);
            }
            if (hasQuorum)
            {
                AcceptRequest acceptRequest = new AcceptRequest();
                acceptRequest.Sender = host;
                acceptRequest.ReadTimestamp = this.read_timestamp;
                acceptRequest.AcceptedValue = this.accepted_value;
                this.state = EnumPaxos.ACCEPT;
                this.nodesList[this.host].AcceptSend(acceptRequest);
            }
            return new PromiseReply
            {
                Ok = true,
            };
        }

        private bool evaluatePromises()
        {
            if (this.promiseArray.Count+1 >= ((nodesList.Count / 2) + 1))
            {
                Console.WriteLine("Existe Quorúm de Promises!!");
                foreach (PromiseRequest r in this.promiseArray)
                {
                    lock (readTimestampLock)
                    {
                        if (r.WriteTimestamp > this.read_timestamp)
                        {
                            Console.WriteLine("Lider estava atrasado! Atualizando os valores!");
                            this.read_timestamp = r.WriteTimestamp;
                            lock (writeTimestampLock)
                            {
                                this.write_timestamp = r.WriteTimestamp;
                            }
                            lock (acceptedValueLock)
                            {
                                this.accepted_value = r.AcceptedValue;
                                this.isUpdated = false;
                            }
                        }
                    }
                }
                if(this.isUpdated)
                    this.accepted_value = this.new_consensus_value;
                return true;
            }
            return false;
        }

        public override Task<AcceptReply> AcceptSend(AcceptRequest request, ServerCallContext context)
        {
            return Task.FromResult(sendAccept(request));
        }

        private AcceptReply sendAccept(AcceptRequest request)
        {
            Console.WriteLine("Sending Accepts...");
            foreach (String p in nodesList.Keys)
            {
                if(!p.Equals(this.host))
                    this.nodesList[p].AcceptReceive(request);
            }
            return new AcceptReply
            {
                Ok = true,
            };
        }

        public override Task<AcceptReply> AcceptReceive(AcceptRequest request, ServerCallContext context)
        {
            return Task.FromResult(receiveAccept(request));
        }

        private AcceptReply receiveAccept(AcceptRequest request)
        {
            Console.WriteLine("Recebi Accept\n  - " + request.Sender + "\n  - " + request.ReadTimestamp + "\n  - " + request.AcceptedValue);
            this.state = EnumPaxos.ACCEPT;
            lock (writeTimestampLock)
            {
                this.write_timestamp = request.ReadTimestamp;
            }
            lock (acceptedValueLock)
            {
                this.accepted_value = request.AcceptedValue;
            }
            this.state = EnumPaxos.INIT;
            AcceptedRequest acceptedRequest = new AcceptedRequest();
            acceptedRequest.Sender = this.host;
            acceptedRequest.Receiver = request.Sender;
            acceptedRequest.WriteTimestamp = this.write_timestamp;
            acceptedRequest.AcceptedValue = this.accepted_value;
            this.nodesList[this.host].AcceptedSend(acceptedRequest);
            return new AcceptReply
            {
                Ok = true,
            };
        }

        public override Task<AcceptedReply> AcceptedSend(AcceptedRequest request, ServerCallContext context)
        {
            return Task.FromResult(sendAccepted(request));
        }

        private AcceptedReply sendAccepted(AcceptedRequest request)
        {
            Console.WriteLine("Enviar Accepted para " + request.Receiver);
            this.nodesList[request.Receiver].AcceptedReceive(request);
            return new AcceptedReply
            {
                Ok = true,
            };
        }

        public override Task<AcceptedReply> AcceptedReceive(AcceptedRequest request, ServerCallContext context)
        {
            return Task.FromResult(receiveAccepted(request));
        }

        private AcceptedReply receiveAccepted(AcceptedRequest request)
        {
            Console.WriteLine("Recebi Accepted\n  - " + request.Sender + "\n  - " + request.WriteTimestamp + "\n  - " + request.AcceptedValue);
            lock (acceptedArrayLock)
            {
                if (this.state.Equals(EnumPaxos.ACCEPT))
                {
                    this.acceptedArray.Add(request);
                    evaluateAccepteds();
                }
                Console.WriteLine(this.acceptedArray.Count);
            }
            return new AcceptedReply
            {
                Ok = true,
            };
        }

        private void evaluateAccepteds()
        {
            if (this.acceptedArray.Count+1 >= ((nodesList.Count / 2) + 1))
                decide();
        }

        private void decide()
        {
            Console.WriteLine("Existe Quorúm de Accepteds!!");
            Console.WriteLine("#####   DECIDE   #####");
            this.promiseArray = new ArrayList();
            this.acceptedArray = new ArrayList();
            this.state = EnumPaxos.INIT;
            this.isUpdated = true;
        }

    }
}
