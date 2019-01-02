# leader election implementation (bully-algorithm)

Any process P can initiate an election
* P sends Election messages to all process with higher IDs and awaits OK messages
  – If no OK messages, P becomes coordinator and sends Coordinator messages to all processes with lower IDs
  – If it receives an OK, it drops out and waits for an Coordinator message
* If a process receives an Election message
  – Immediately sends Coordinator message if it is the process with highest ID
  – Otherwise, returns an OK and starts an election
* If a process receives a Coordinator message, it treats sender as the coordinator
