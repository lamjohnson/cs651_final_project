# Party Distributed Processing
### BYOB: Bring Your Own Bits
#### Authors: Johnson Lam and Justin Chen 
#### BUCS CS651: Distributed Systems
#### Writeup: [Link]{https://github.com/jlam17/cs651_final_project/blob/master/writeup/party-distributed-processing.pdf}

### Setup for Linux and OSX
Setup so that the worker automatically launches when the system initially starts up. This is not necessary so skip to the next step if you'd like. 
For OSX:
```
$ nano /.bash_profile
```
For Linux:
```
$ nano /.bashrc
```
Appending the following to the file and replace `<abs path>` with absolute path to the `party.sh` file:
```
$ export GOROOT=/usr/local/go/
$ export PATH=$PATH:$GOROOT/bin
$ sh <abs path>/party.sh
```

### To run on machine
To test on local, open one terminal for the master, and a terminal for each worker. Make sure you ran the above steps to get the exports. Otherwise run the following exports in each terminal:

```
export GOROOT=/<your go directory>/go
export PATH=/$PATH:$GOROOT/bin
export GOPATH=$HOME/<master/worker directory/
```

Now run the master and worker files

In the master's terminal:
```
$ go run master.go
```

In the workers' terminals:
```
$ go run worker.go
```

Todo in order of priority:
- Take care of networks errors (worker and master side) 
- Add fault tolerance in case master get K.O.'d in which case another server should assume role of master (use raft) 
- Workers shoud track the state of their jobs and periodically ping their progress to the master often enough so that the master can compensate if something happens to that worker


Secondary:
- Script to automatically lauch worker when computer turns on
- Worker should have a standard form for submitting jobs containing the data source, data type, dataset size, comment about job, which master can disseminate to other workers
- Workers should be able to limit their resources according to the config file
- Extend functionality to process training data 
- Handle concurrent job requests 
- Scoring workers and allocating work to those more suitable 

Other:
- Master should authenticate members joining party
- Master should have cached file containing the party password, which all members agree on before hand for join the party, and all invited members

Completed:
- Master should be able to aggregate results from each worker as workers finish
- Periodic heartbeats for worker and master 
- Master should track be able to track workers as they join and leave party
- Worker should be able to submit a job to the master
- Master should divide up file 
- Master should be able to queue jobs, and tell members which job they should currently be working on
- Master should store final result in designated location and report to submitter that job is complete. Don't need to tell every worker that job is complete, just the one that submitted the orignal job.
- Jobs should be limited to word count for now as in the mapreduce homework
- Host Master and Worker on server (get routing working for ip addresses/ports on external network) 
