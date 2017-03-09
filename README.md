# Party Distributed Processing
### BYOB: Bring Your Own Bits
#### Authors: Johnson Lam and Justin Chen 
#### BUCS CS651: Distributed Systems


To test on local, open one terminal for the master, and a terminal for each worker. In the master's terminal:
```
$ go run master.go
```

In the workers' terminals:
```
$ go run worker.go
```

Todo in order of priority:
- Master should have cached file containing the party password, which all members agree on before hand for join the party, and all invited members
- Master should authenticate members joining party
- Master should track be able to track workers as they join and leave party
- Script to automatically lauch worker when computer turns on
- Worker should have a standard form for submitting jobs containing the data source, data type, dataset size, comment about job, which master can disseminate to other workers
- Worker should be able to submit a job to the master
- Master should be able to queue jobs, and tell members which job they should currently be working on
- Master should allocate appropriately sized jobs to each worker based on their configurations submitted when they last joined the current session
- Master should divide dataset up into cunks and tell each worker where in the byte stream to start reading (not sure if this will be our exact approach. I guess we'll find out...) 
- Workers shoud track the state of their jobs and periodically ping their progress to the master often enough so that the master can compensate if something happens to that worker
- Master should be able to aggregate results from each worker as workers finish
- Master should store final result in designated location and report to submitter that job is complete. Don't need to tell every worker that job is complete, just the one that submitted the orignal job.
- Add fault tolerance in case master get K.O.'d in which case one of the workers should assume role of master
- Workers should be able to limit their resources according to the config file
- Jobs should be limited to word count for now as in the mapreduce homework
