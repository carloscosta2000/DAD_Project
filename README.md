# DAD Project Checkpoint

## Group 1
Carlos Costa 105768 \
Francisco Fiães 105776 \
Duarte Costa 105787

## How to run the project
If there are any issues with the gRPC or Protobuf packages please clean the solution and rebuild the project.
The process that spawns the infrastructure is under the ***Control*** project, so that is the .exe/Startup configuration that should be run.

## What was implemented
The full system architecture is implemented. Clients are submitting transactions to transaction managers and these transactions are being stored locally in the transaction manager the transaction was submitted to. Transaction managers also make lease requests to lease managers, and these leases are assigned through the use of Paxos when there are conflicting keys. We are not yet dealing with any crashes in transactions and lease managers.