using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionManager
{
    public class CTMService : TransactionService.TransactionServiceBase
    {
        private TransactionManagerClientLogic TMClientLogic;
        private TransactionManager.TransactionsManager transactionManager;
        private string name;
        private HashSet<int> crashed_time_slots;
        private TimeStepInfo timeStepInfo;

        public CTMService(TransactionManagerClientLogic TMClientLogic, string name)
        {
            this.TMClientLogic = TMClientLogic;
            this.name = name;
            this.transactionManager = TransactionManager.TransactionsManager.Instance(name);
            this.timeStepInfo = TimeStepInfo.Instance;
        }

        public override Task<TransactionSubmitReply> TxSubmit(TransactionSubmitRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : TxSubmit - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            
            List<DadInt> write_keys = new List<DadInt>(request.Writes.ToArray());
            TransactionSubmitReply reply = new TransactionSubmitReply();
            List<DADTKV.DadInt> reads = this.transactionManager.getReads(request.Reads.ToArray());
            foreach (DADTKV.DadInt dadInt in reads)
                reply.Reads.Add(new DadInt { Key = dadInt.Key, Value = dadInt.Value });
            List<string> keys_to_write = new List<string>();
            StringBuilder sb = new StringBuilder();
            sb.Append("#####   WRITE OPERATIONS received: ");
            foreach(DadInt key in write_keys)
            {
                sb.Append(key + ", ");
                keys_to_write.Add(key.Key);
            }
            Console.WriteLine(sb.ToString() + "   #####");    

            //check if there are leftovers of heldleases
            List<string> leftover_keys = keys_to_write.Except(this.transactionManager.getHeldLeases()).ToList();

            //if the TM has the lease
            foreach (DadInt dadInt in write_keys)
                if (this.transactionManager.getHeldLeases().Contains(dadInt.Key))
                    this.transactionManager.writeInKeyWithoutLeases(dadInt.Key, dadInt.Value);
            
            //if the TM doesn't have the lease
            Transactions transaction_to_LM = new Transactions { Id = this.name };
            foreach (string read in request.Reads)
                transaction_to_LM.Reads.Add(read);

            foreach (DadInt write in request.Writes)
            {
                TransactionDadInt newDadInt = new TransactionDadInt { Key = write.Key, Value = write.Value};
                transaction_to_LM.Writes.Add(newDadInt);
            }

            TMClientLogic.sendLeaseBroadCast(leftover_keys, transaction_to_LM);
            return Task.FromResult(reply);
        }
    }

    public class LAService : LeaseAssignService.LeaseAssignServiceBase
    {
        private string username;
        private string hostname;
        private TimeStepInfo timeStepInfo;
        public LAService(string username, string hostname)
        {
            this.username = username;
            this.hostname = hostname;
            this.timeStepInfo = TimeStepInfo.Instance;
        }
        public override Task<ResponseDecidedLeases> DecideLeases(
            RequestDecidedLeases request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : DecideLeases - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            TransactionsManager transactionsManager = TransactionsManager.Instance(this.username);
            transactionsManager.decideLeases(request);
            return Task.FromResult(saveDecidedLeases(request));
        }

        public ResponseDecidedLeases saveDecidedLeases(RequestDecidedLeases request)
        {
            return new ResponseDecidedLeases
            {
                Id = this.username,
                Hostname = this.hostname,
                Status = true
            };
        }

    }

    public class SService : StatusService.StatusServiceBase
    {
        private TimeStepInfo timeStepInfo;
        private string name;
        private TransactionManager.TransactionsManager transactionManager;


        public SService(string name)
        {
            this.name = name;
            this.timeStepInfo = TimeStepInfo.Instance;
            this.transactionManager = TransactionManager.TransactionsManager.Instance(name);

        }

        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {
            string state = "";
            if (this.timeStepInfo.isCrashed())
                state = "C";
            else
                state = "N";
            StatusReply statusReply = new StatusReply
            {
                InstanceId = this.name,
                State = state,
                CurrentTimeSlot = this.timeStepInfo.get_current_timestep(),
                Type = "TM",
            };
            foreach (string s in this.timeStepInfo.get_suspected_time_slots()[this.timeStepInfo.get_current_timestep()])
                statusReply.Suspected.Add(s);
            foreach (DADTKV.DadInt dadInt in this.transactionManager.getState())
                statusReply.CurrentState.Add(new DadIntResult { Key = dadInt.Key, Value = dadInt.Value });


            return Task.FromResult(statusReply);
        }

    }

    public class RepService : ReplicationService.ReplicationServiceBase
    {
        private string username;
        private Dictionary<string, ReplicationService.ReplicationServiceClient> tmList;
        private ProcessesState processesState;
        private TimeStepInfo timeStepInfo;

        public RepService(string username, Dictionary<string, string> tms)
        {
            this.username = username;
            this.timeStepInfo = TimeStepInfo.Instance;
            this.tmList = new Dictionary<string, ReplicationService.ReplicationServiceClient>();
            foreach (KeyValuePair<string, string> entry in tms)
                this.tmList.Add(entry.Key, new ReplicationService.ReplicationServiceClient(GrpcChannel.ForAddress("http://" + entry.Value)));
            this.processesState = ProcessesState.Instance;
            this.processesState.initDataStructure(tmList.Keys.ToList());
        }
        public override Task<ReplicateReply> SendReplicate(ReplicateRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : SendReplicate - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return sendReplicate(request);
        }

        private async Task<ReplicateReply> sendReplicate(ReplicateRequest request)
        {
            List <ReplicateReply> listReplies = new List<ReplicateReply>();
            foreach (string tm in tmList.Keys)
                if (!tm.Equals(username) && !this.timeStepInfo.isSuspect(tm))
                {
                    try
                    {
                        //ReceiveReplicateAsync returns a reply count the replys....
                        ReplicateReply reply = await this.tmList[tm].ReceiveReplicateAsync(request, 
                            new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                        listReplies.Add(reply);
                    }
                    catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                    {
                        this.processesState.suspectProcess(tm);
                        Console.WriteLine("ReceiveReplicateAsync timeout. Suspecting process " + tm);
                    }
                }
            ReplicateReply replyBool = new ReplicateReply { Ok = false };
            if (listReplies.Count >= ((tmList.Count / 2) + 1))
                replyBool.Ok = true;
            return replyBool;
        }

        public override Task<ReplicateReply> ReceiveReplicate(ReplicateRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : ReceiveReplicate - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receiveReplicate(request));
        }

        private ReplicateReply receiveReplicate(ReplicateRequest request)
        {
            TransactionsManager transactionsManager = TransactionsManager.Instance(this.username);
            transactionsManager.updateState(request);
            return new ReplicateReply
            {
                Ok= true
            };
        }

        public override Task<RecoverReply> SendRecoverRequest(RecoverRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : SendRecoverRequest - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendRecoverRequest(request));
        }

        private RecoverReply sendRecoverRequest(RecoverRequest request)
        {
            foreach (string tm in tmList.Keys)
            {
                if (!tm.Equals(username) && !this.timeStepInfo.isSuspect(tm))
                {
                    try
                    {
                        this.tmList[tm].ReceiveRecoverRequestAsync(request,
                            new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                    }
                    catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                    {
                        this.processesState.suspectProcess(tm);
                        Console.WriteLine("ReceiveRecoverRequestAsync timeout. Suspecting process " + tm);
                    }
                }
            }
            return new RecoverReply
            {
                Ok = true,
            };
        }

        public override Task<RecoverReply> ReceiveRecoverRequest(RecoverRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : ReceiveRecoverRequest - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receiveRecoverRequest(request));
        }

        private RecoverReply receiveRecoverRequest(RecoverRequest request)
        {
            TransactionsManager transactionsManager = TransactionsManager.Instance(this.username);
            transactionsManager.receiveRecoverRequest(request);
            return new RecoverReply
            {
                Ok = true
            };
        }

        public override Task<StateReply> SendStateRequest(StateRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : SendStateRequest - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendStateRequest(request));
        }

        private StateReply sendStateRequest(StateRequest request)
        {
            if (!this.timeStepInfo.isSuspect(request.Receiver))
            {
                try
                {
                    this.tmList[request.Receiver].ReceiveStateRequestAsync(request,
                        new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    this.processesState.suspectProcess(request.Receiver);
                    Console.WriteLine("ReceiveStateRequestAsync timeout. Suspecting process " + request.Receiver);
                }
            }  
            return new StateReply
            {
                Ok = true,
            };
        }

        public override Task<StateReply> ReceiveStateRequest(StateRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : ReceiveStateRequest - TransactionManagerServerLogic");
                return null;
            }
            return Task.FromResult(receiveStateRequest(request));
        }

        private StateReply receiveStateRequest(StateRequest request)
        {
            TransactionsManager transactionsManager = TransactionsManager.Instance(this.username);
            transactionsManager.receiveStateRequest(request);
            return new StateReply
            {
                Ok = true
            };
        }

    }

    public class RecService : RecoverService.RecoverServiceBase
    {
        private string username;
        private Dictionary<string, RecoverService.RecoverServiceClient> tmList;
        private ProcessesState processesState;
        private TimeStepInfo timeStepInfo;
        private TransactionManager.TransactionsManager transactionManager;
        private List<UpdateStateRequest> checkQuorumUpdate;
        private static readonly object checkQuorumUpdateLock = new object();
        public RecService(string username, Dictionary<string, string> tms) {
            this.username = username;
            this.processesState = ProcessesState.Instance;
            this.tmList = new Dictionary<string, RecoverService.RecoverServiceClient>();
            foreach (KeyValuePair<string, string> entry in tms)
                this.tmList.Add(entry.Key, new RecoverService.RecoverServiceClient(GrpcChannel.ForAddress("http://" + entry.Value)));
            this.transactionManager = TransactionsManager.Instance(this.username);
            this.checkQuorumUpdate = new List<UpdateStateRequest>();
            this.timeStepInfo = TimeStepInfo.Instance;
        }

        public override Task<CrashRecoverReply> SendCrashRecoverRequest(CrashRecoverRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : SendCrashRecoverRequest - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(sendCrashRecoverRequest(request));
        }

        private CrashRecoverReply sendCrashRecoverRequest(CrashRecoverRequest request)
        {
            lock (checkQuorumUpdateLock)
            {
                this.checkQuorumUpdate.Clear();
            }
            foreach (string tm in tmList.Keys)
            {
                if (!tm.Equals(this.username) && !this.timeStepInfo.isSuspect(tm))
                {
                    try
                    {
                        this.tmList[tm].ReceiveCrashRecoverRequestAsync(request,
                            new CallOptions(deadline: DateTime.UtcNow.AddSeconds(this.processesState.DEADLINE)));
                    }
                    catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                    {
                        this.processesState.suspectProcess(tm);
                        Console.WriteLine("ReceiveCrashRecoverRequestAsync timeout. Suspecting process " + tm);
                    }
                }
            }
            return new CrashRecoverReply
            {
                Ok = true,
            };
        }

        public override Task<CrashRecoverReply> ReceiveCrashRecoverRequest(CrashRecoverRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : ReceiveCrashRecoverRequest - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receiveCrashRecoverRequest(request));
        }

        private CrashRecoverReply receiveCrashRecoverRequest(CrashRecoverRequest request)
        {
            List < DADTKV.DadInt > state = this.transactionManager.getState();
            RepeatedField<DadIntRecover> stateRecover = new RepeatedField<DadIntRecover>();
            UpdateStateRequest updateRequest = new UpdateStateRequest
            {
                Sender = this.username
            };
            foreach (DADTKV.DadInt s in state)
                updateRequest.State.Add(new DadIntRecover
                {
                    Key = s.Key,
                    Value = s.Value
                });
            this.tmList[request.Sender].ReceiveUpdateStateAsync(updateRequest);
            return new CrashRecoverReply
            {
                Ok = true,
            };
        }

        public override Task<UpdateStateReply> ReceiveUpdateState(UpdateStateRequest request, ServerCallContext context)
        {
            if (this.timeStepInfo.isCrashed())
            {
                Console.WriteLine("CRASHED in " + this.timeStepInfo.get_current_timestep() + " : ReceiveUpdateState - TransactionManagerServerLogic");
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, ""));
            }
            return Task.FromResult(receiveUpdateState(request));
        }

        private UpdateStateReply receiveUpdateState(UpdateStateRequest request)
        {
            lock (checkQuorumUpdateLock)
            {
                this.checkQuorumUpdate.Add(request);
                if (this.checkQuorumUpdate.Count >= ((tmList.Count / 2) + 1))
                {
                    this.transactionManager.recoverState(request);
                    this.checkQuorumUpdate.Clear();
                }
            }
            return new UpdateStateReply { Ok = true };
        }  
    }
}