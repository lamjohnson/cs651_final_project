/*
    master.go
    authors: Justin Chen, Johnson Lam
    
    Master interface to party.

    4.19.17
	Currently implements word count. Future work to be done to extend functionality.
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
	"bytes"
)

const create = "create"
const reset	 = "reset"
const close  = "close"
const fileSize = 1000

// TODO: Move into master struct and instantiate a master state instance 
var currentWorkers	map[int]*WorkerState //key: worker_id
var currentJobs 	map[int]*WorkerProgress //key: worker_id
var totalFiles 		int 
var finishedFiles 	int 
var finalTotal 		int 

var muWorkers		sync.Mutex
var muJobs			sync.Mutex
var muActive		sync.Mutex
var nextWorker 		chan int
var nextJob 		chan FileEntry
var activeJob	 	bool 
var finishedAll 	chan int


// initial job request
type JobRequest struct {
	Filename 	string
	Url			string
	Worker_id 	int 
	Ip 			string 
	Port 		int 
	Total		int
}

// file chunk sent to worker
type JobArgs struct {
	Worker_id 	int
	File 		FileEntry
}

// worker reply for file chunk
type JobReply struct {
	Worker_id 	int 
	File_id 	string	
	Progress 	int 
}

// TODO: Create master object and associate functions with this type
type MasterState struct {
	
}

// TODO: Associate functions with this type
type WorkerState struct {
	WorkerId	int
	Ip 			string 
	Port		int
	Timeout		*time.Timer
	Heartbeat 	chan bool
	Failures 	int 
}

// worker to file progress 
type WorkerProgress struct {
	file 		FileEntry	
	progress 	int 
	total		int 
}

type FileEntry struct {
	Filename 	string 
	Data 		string
}

//
// Functions for handling requests from workers 
//
func chunkHandler(w http.ResponseWriter, req *http.Request) {
	job, err :=  decodeJob(req) 
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
	}
	finalTotal += job.Progress
	lastProgress := currentJobs[job.Worker_id]
	lastProgress.progress += job.Progress
	printl("Worker %v finished job with progress %v out of %v", job.Worker_id, lastProgress.progress, lastProgress.total)

	// TODO: Not sure if need check reply received matches work given 
	if lastProgress.file.Filename == job.File_id {
		printl("Removing active job")
		delete(currentJobs, job.Worker_id)
		os.Remove(job.File_id) 
		finishedFiles++
	}

	// TODO: Think of smarter way to check that job is complete. Maybe use a map?
	if finishedFiles == totalFiles {
		finishedFiles = 0
		// Check: Is it necessary to check for lingering jobs. Why would there be any?
		if len(currentJobs) > 0 {
			for k := range currentJobs {
				delete(currentJobs, k)
			}
		}
		changeStatus(false)
		finishedAll <- 0
		return
	}

	go func(worker_id int) {
		nextWorker <- worker_id
	} (job.Worker_id)
}

// TODO: Include file or filename url to retreive 
// TODO: Reject if there is an active job 
// - Later on specify type of work to be done 
func submitRequest(w http.ResponseWriter, req *http.Request) {
	job, err := decodeRequest(req)
	if err != nil {
        http.Error(w, err.Error(), 400)
        return
	}
	ok := changeStatus(true)
	if !ok {
		// Reply to client that you're busy
    	io.WriteString(w, "Master is currently busy with a request, please try again later...\n")
		return 
	}
    io.WriteString(w, "Request submitted successfully, processing...\n")
	finalTotal = 0 
	download_err := downloadFile( job.Filename, job.Url)
	if download_err != nil {
		io.WriteString(w, fmt.Sprintf("Error %v with downloading file, please try again\n", download_err))
		return
	}
	files := splitFile(job.Filename, fileSize)
	workers := getAvailWorkers()
	nextWorker = make(chan int)
	nextJob = make(chan FileEntry)
	finishedAll = make(chan int)

	printl("Got request %v from worker %v", job, job.Worker_id)
	printl("There are %v files", totalFiles)

	// Handout jobs accordingly 
	go func(nextWorker <-chan int, nextJob <- chan FileEntry, finishedAll <-chan int, job JobRequest ) {
		printl("Starting thread to hand out file chunks to workers")
		Loop:
			for {
				select {
				case file := <-nextJob:
					printl("Waiting to send %v", file.Filename)
					worker_id := <- nextWorker
					sendChunk(worker_id, file, fileSize)
					printl("Sent file %v", file.Filename)
				case <- finishedAll:
					printl("Finished job, final total: %v", finalTotal)
					break Loop 
				}
			}
		printl("Finished handing out file chunks")
		sendNotif(job, finalTotal)
	}(nextWorker, nextJob, finishedAll, job)

	// Treating channel as a queue to put jobs and workers available 
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
}


// TODO: Check progress of workers and update currentJobs accordingly 
// 		- Fun visualization: github.com/cheggaaa/pb
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

func join(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeConfig(req) 
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID
	if _, ok := currentWorkers[uid]; ok {
		log.Fatal("Intiailzing a worker that exists...")
	}

	printl("Worker %v joining master\n", conf.Id.Alias)
	initializeWorker(conf)

	// add worker to queue if there is an active job 
	muActive.Lock()
	if activeJob {
		go func() {
			nextWorker <- uid
		} ()
	}
	muActive.Unlock()

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

func sendChunk(worker_id int, file FileEntry, file_size int) {
	muJobs.Lock()
	job := WorkerProgress{file, 0, file_size}
	currentJobs[worker_id] = &job 
	muJobs.Unlock() 

	jobArgs := JobArgs{worker_id, file} 

	muWorkers.Lock()
	go func(worker *WorkerState, jobArgs *JobArgs) {
		base := fmt.Sprintf("http://%v:%v", worker.Ip, worker.Port)
		url := base + "/process_chunk"
		printl("Sending file chunk %v to worker %v at url %v", file.Filename, worker_id, url)
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
	_, err := http.Post(url,"application/json; charset=utf-8", b)
	if err != nil {
		time.Sleep(1 * time.Second)
		sendNotif(request, result)
	}
}

//					  //
// Internal Functions //
//					  //

func downloadFile(filepath string, url string) error {
	// Create file
	out, err := os.Create(filepath)
	if err != nil {
		printl("error 1")
		return err 
	}
	defer out.Close()

	// Get data
	resp, err := http.Get(url)
	if err != nil {
		printl("error 2 %v", url)
		return err
	}
	defer resp.Body.Close()

	// Write data to file 
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		printl("error 3")
		return err
	}

	return nil
}
func changeStatus(active bool) bool {
	muActive.Lock()
	if (active && !activeJob) || !active {
		activeJob = active
		muActive.Unlock()
		return true
	}
	muActive.Unlock()
	return false
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

	totalFiles = len(names)
	return names 

 }           

func initializeMaster() {
	currentWorkers = make(map[int]*WorkerState)
	currentJobs = make(map[int]*WorkerProgress)
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
					muJobs.Lock()
					w, active := currentJobs[worker.WorkerId]
					muJobs.Unlock()

					if worker.Failures < 3 && !active {
						printl("Failed to hear from worker %v %v time(s)", worker.WorkerId, worker.Failures)
						worker.Timeout = setTimer(reset, 5, worker.Timeout) 
						worker.Failures++
					} else {
						if active {
							printl("Adding unfinished job %v to queue", w.file.Filename)
							go func() {
								nextJob <- w.file
							} ()
						}

						printl("Deleting worker %v from active workers list\n", worker.WorkerId)
						muWorkers.Lock()
						delete(currentWorkers, server_id)
						muWorkers.Unlock()
						printl("Timer expired on worker %v\n", server_id)
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
	log.Printf(format + "\n", a...)
}

func main() {
	printl("Listening on port 8080...")
	initializeMaster()

	go sendHeartbeat()

    http.HandleFunc("/job_request", submitRequest)
    http.HandleFunc("/join", join)
	http.HandleFunc("/m_heartbeat", heartbeatHandler) 
	http.HandleFunc("/job_chunk", chunkHandler) 
    http.ListenAndServe(":8080", nil)
}
