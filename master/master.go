/*
    master.go
    authors: Justin Chen, Johnson Lam
    
    Master interface to party.
	init_state.Port = conf.Party.Port

    3.7.17
*/

package main

import (
    "fmt"
    "net/http"
    "net"
    "io"
	"os"
	"bufio"
    "config"
    "encoding/json"
	"time" 
	"log"
	"sync"
	"crypto/rand"
	"math/big"
	"bytes"
	// "strings"
)

var expiredServers 	chan int 
var currentWorkers	map[int]*WorkerState //key: worker_id
var currentJobs 	map[int]*WorkerProgress //key: worker_id
var activeJob	 	bool 
var totalFiles 		int 
var finishedFiles 	int 
var muWorkers		sync.Mutex
var muJobs			sync.Mutex
var nextWorker 		chan int
var nextJob 		chan FileEntry
var finishedAll 	chan int
var finalTotal 		int 

const create = "create"
const reset	 = "reset"
const close  = "close"
const nNumber = 1000

type JobRequest struct {
	Filename 	string
	Worker_id 	int 
	Ip 			string 
	Port 		int 
	Total		int
}

type JobArgs struct {
	Worker_id 	int
	File_id  	string	
	File 		string 
}

type JobReply struct {
	Worker_id 	int 
	File_id 	string	
	Progress 	int 
}

type WorkerState struct {
	WorkerId	int
	Ip 			string 
	Port		int
	Timeout		*time.Timer
	Heartbeat 	chan bool
	Failures 	int 
}

type WorkerProgress struct {
	file_id 	string
	progress 	int 
	total		int 
}

type MasterState struct {
	
}

type FileEntry struct {
	Filename 	string 
	Data 		string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//
// Functions for handling requests from workers 
//
// - DONE: Mark chunk of file as finished
// - TODO: Send next chunk (do in submit job handler) 
func chunkHandler(w http.ResponseWriter, req *http.Request) {
	job, err :=  decodeJob(req) 
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
	}
	finalTotal += job.Progress
	finishedFiles++

	lastProgress := currentJobs[job.Worker_id]
	printl("Worker: %v", job.Worker_id)
	printl("Current job progress: %v", currentJobs)
	if lastProgress.file_id == job.File_id {
		printl("Found job in current jobs")
		delete(currentJobs, job.Worker_id)
		os.Remove(job.File_id) 
		finishedFiles++
	}

	// activeJob = false
	if finishedFiles == totalFiles || !activeJob {
		//TODO: Reply to client that job has finished along with results 	
		finishedAll <- 0
		printl("FINISHED")
		return
	}

	go func(worker_id int) {
		nextWorker <- worker_id
	} (job.Worker_id)
}

// look at mapreduce code 
// - No need to specify work type, assume word count 
//     - Later on specify type of work to be done 
// - Include file or filename url to retreive 
// - Divide up file into chunks and send to workers 
func submitRequest(w http.ResponseWriter, req *http.Request) {
	job, err := decodeRequest(req)
	if err != nil {
        http.Error(w, err.Error(), 400)
        return
	}
	printl("Got request %v", job)

	activeJob = true 
	files := splitFile(job.Filename, nNumber)
	totalFiles = len(files)
	printl("There are %v files", totalFiles)
	workers := getAvailWorkers()
	nextWorker = make(chan int)
	nextJob = make(chan FileEntry)
	finishedAll = make(chan int)
	finalTotal = 0 
	go func(nextWorker <-chan int, nextJob <- chan FileEntry, finishedAll <-chan int, job JobRequest ) {
		printl("Starting thread to hand out file chunks to workers")
		Loop:
			for {
				select {
				case file := <-nextJob:
					printl("Waiting to send %v", file.Filename)
					worker_id := <- nextWorker
					sendChunk(worker_id, file.Filename, file.Data, nNumber)
					printl("Sent file %v", file.Filename)
				case <- finishedAll:
					printl("Final total: %v", finalTotal)
					break Loop 
				}
			}
		printl("Finished handing out file chunks")
		sendNotif(job, finalTotal)
	}(nextWorker, nextJob, finishedAll, job)
	// nextJob <- files[0]
	go func() {
		for _, job := range files {
			nextJob <- job
		}
	} () 
	go func() {
		for _, worker := range workers {
			nextWorker <- worker
		}
	} ()
	// nextWorker <- workers[0]
}

// - Check availability of worker 
// - Check progress of workers (github.com/cheggaaa/pb)
func heartbeatHandler(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeConfig(req)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID
	if _, ok := currentWorkers[uid]; !ok {
		log.Fatal("Worker expired...reinitialze worker...")
	}
	currentWorkers[uid].Heartbeat <- true 
}

// - Config: worker machine includes available dependencies 
func join(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeConfig(req) 
	// ip, _ := getIP(req)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID
	if _, ok := currentWorkers[uid]; ok {
		log.Fatal("Intiailzing a worker that exists...")
	}

	fmt.Printf("Worker %v joining master\n", conf.Id.Alias)
	initializeWorker(conf)

    // Receipt for client joining the party
    io.WriteString(w, "Welcome to the party, "+conf.Id.Alias+"\n")
}
//
// Functions for sending requests to worker 
//
func sendHeartbeat() {
	for {
		muWorkers.Lock()
		for worker := range currentWorkers {
			go func(worker *WorkerState) {
				base := fmt.Sprintf("http://%v:%v", worker.Ip, worker.Port)
				url := base + "/w_heartbeat"
				printl("Pinging worker %v at %v", worker.WorkerId, url)
				http.Get(url)
			} (currentWorkers[worker])
		}
		muWorkers.Unlock()
		time.Sleep(3 * time.Second)
	}
}

func sendChunk(worker_id int, file_id string, file string, file_size int) {
	muJobs.Lock()
	job := WorkerProgress{file_id, 0, file_size}
	currentJobs[worker_id] = &job
	muJobs.Unlock()

	jobArgs := JobArgs{worker_id, file_id, file}

	muWorkers.Lock()
	go func(worker *WorkerState, jobArgs *JobArgs) {
		printl("Sending file chunk %v to worker %v", file_id , worker_id)
		base := fmt.Sprintf("http://%v:%v", worker.Ip, worker.Port)
		url := base + "/process_chunk"
		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(&jobArgs)
		http.Post(url,"application/json; charset=utf-8", b)
	}(currentWorkers[worker_id], &jobArgs)
	muWorkers.Unlock()
}

func sendNotif(request JobRequest, result int) {
	base := fmt.Sprintf("http://%v:%v", request.Ip, request.Port)
	url := base + "/finish_job"
	request.Total = result
	printl("Notifying worker %v that file %v has %v words at url %v", request.Worker_id, request.Filename, request.Total, url) 
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(&request)
	http.Post(url,"application/json; charset=utf-8", b)
}

//					  //
// Internal Functions //
//					  //
func addWorker(worker_id int) {

}

func readWorker(worker_id int) {

}

func getAvailWorkers() []int {
	workers := make([]int,0, len(currentWorkers))	
	for worker := range currentWorkers {
		workers = append(workers, currentWorkers[worker].WorkerId)
	}
	printl("Workers: %v", workers)
	return workers 
}

func getIP(req *http.Request) (net.IP, error) {
    ip, _, err := net.SplitHostPort(req.RemoteAddr)
    if err != nil {
        return nil, fmt.Errorf("userip: %q is not IP:port", req.RemoteAddr)
    }
    userIP := net.ParseIP(ip)
    if userIP == nil {
        return nil, fmt.Errorf("userip: %q is not IP:port", req.RemoteAddr)
    }
    return userIP, nil
}

func decodeRequest(req *http.Request) (JobRequest, error) {
	var request JobRequest
    err := json.NewDecoder(req.Body).Decode(&request)
	return request, err
}

func decodeJob(req *http.Request) (JobReply, error) {
    var job JobReply
    err := json.NewDecoder(req.Body).Decode(&job)
	return job, err
}

func decodeConfig(req *http.Request) (config.Configuration, error) {
    var conf config.Configuration
    err := json.NewDecoder(req.Body).Decode(&conf)
	return conf, err
}

func setTimer(command string, length int, timeout *time.Timer) *time.Timer {
	switch command {
	case close: 
		timeout.Stop()
	case reset:	
		timeout.Stop()
		timeout.Reset(time.Duration(length)* time.Second)
	case create:
		timeout = time.NewTimer(time.Duration(length)* time.Second)
	}
	return timeout 
}


func splitFile(filename string, file_length int) []FileEntry {    
	var names []FileEntry
	file_input, err := os.Open(filename)
	reader:= bufio.NewReader(file_input)
	file_count := 0 
	line_count := 0

	names = append(names, FileEntry{fmt.Sprintf(filename + "-%d.txt", file_count), ""}) 
	file, err := os.Create(names[file_count].Filename)
	if err != nil {                
		log.Fatal("mkInput: ", err)
	}                              
	w := bufio.NewWriter(file)     
	line, err := reader.ReadString('\n')

	for {
		w.WriteString(line)
		names[file_count].Data += line 
		line_count++

		// Create new file when line limit per file is reached 
		if line_count % file_length == 0 {
			w.Flush()
			file.Close()

			file_count++
			names = append(names, FileEntry{fmt.Sprintf("filename-%d.txt", file_count),""}) 
			file, err = os.Create(names[file_count].Filename)
			if err != nil {                
				log.Fatal("mkInput: ", err)
			}                              
			w = bufio.NewWriter(file)     
		}

		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
	}

	// Close last file 
	if line_count % file_length != 0 {
		w.Flush()
		file.Close()
	}
	file_input.Close()

	return names 

 }           
// Run a thread for handling expired workers 
func notifyExpired() {
	for {
		select {
		case server_id := <- expiredServers:
			fmt.Printf("Timer expired on worker %v\n", server_id)
		} 
	}
}

func initializeMaster() {
	currentWorkers = make(map[int]*WorkerState)
	currentJobs = make(map[int]*WorkerProgress)
	expiredServers = make(chan int) 
	activeJob = false 
	finalTotal = 0
	finishedFiles = 0
}

func initializeWorker(conf config.Configuration ) *WorkerState {
	var init_state WorkerState  
	init_state.WorkerId = conf.Id.UID
	init_state.Ip= conf.Party.IP
	init_state.Port = conf.Party.Port
	init_state.Heartbeat = make(chan bool)
	init_state.Failures = 0 
	
	currentWorkers[init_state.WorkerId] = &init_state
	currentWorkers[init_state.WorkerId].Timeout = setTimer(create, 5, init_state.Timeout)

	go func(server_id int, worker *WorkerState) {
		Loop:
			for {
				select {
				case <- worker.Timeout.C:
					if worker.Failures < 3 {
						printl("Failed to hear from worker %v %v time(s)", worker.WorkerId, worker.Failures)
						worker.Timeout = setTimer(reset, 5, worker.Timeout) 
						worker.Failures++
					} else {
						fmt.Printf("Deleting worker from active workers list\n")
						muWorkers.Lock()
						delete(currentWorkers, server_id)
						muWorkers.Unlock()

						expiredServers <- server_id
						break Loop
					}
				case <- worker.Heartbeat:
					worker.Timeout = setTimer(reset, 5, worker.Timeout) 
					worker.Failures = 0 
				}
			}
	}(init_state.WorkerId, &init_state)

	return &init_state 
}

func printl(format string, a ...interface{}) {
	fmt.Printf(format + "\n", a...)
}

func main() {
	fmt.Println("Listening on port 8080...")
	initializeMaster()

	go notifyExpired()
	go sendHeartbeat()

    http.HandleFunc("/job_request", submitRequest)
    http.HandleFunc("/join", join)
	http.HandleFunc("/m_heartbeat", heartbeatHandler) 
	http.HandleFunc("/job_chunk", chunkHandler) 
    http.ListenAndServe(":8080", nil)
}
