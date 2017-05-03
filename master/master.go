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
const minProgress = fileSize/10

// TODO: Move into master struct and instantiate a master state instance 
var currentWorkers	map[int]*WorkerState //key: worker_id
var currentJobs 	map[int]*WorkerProgress //key: worker_id
var finishedJobs 	map[string]struct{} //key: file_chunk_id (at  most once result)
var totalFiles 		int 
var finishedFiles 	int 
var finalTotal 		int 
var submitter 		int 

var muWorkers		sync.Mutex
var muJobs			sync.Mutex
var muActive		sync.Mutex
var muFinishedJob 	sync.Mutex
var nextWorker 		chan int
var nextJob 		chan FileEntry
var activeJob	 	bool 
var submitterFailure chan int 
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
	Result 		int 
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
	Size 		int 
}

type Heartbeat struct {
	Id 			config.Identity 
	Progress	int
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
	muJobs.Lock()
	lastProgress, ok:= currentJobs[job.Worker_id]
	muJobs.Unlock()
	if !ok {
		printl("Job not found...check that submitter died or job has finished")
		return
	}

	muFinishedJob.Lock()
	_, finished := finishedJobs[job.File_id]
	if finished {
		printl("Job %v already processed", job.File_id)
		muFinishedJob.Unlock()
		go func(worker_id int) {
			nextWorker <- worker_id
		} (job.Worker_id)
		return
	}
	var empty struct{}
	finishedJobs[job.File_id] = empty
	muFinishedJob.Unlock()

	finalTotal += job.Result
	lastProgress.progress = job.Progress
	printl("Worker %v finished job %v with progress %v out of %v", job.Worker_id, job.File_id, lastProgress.progress, lastProgress.total)

	// TODO: Not sure if need check reply received matches work given 
	if lastProgress.file.Filename == job.File_id {
		printl("Removing active job")
		muJobs.Lock()
		delete(currentJobs, job.Worker_id)
		muJobs.Unlock()
		os.Remove(job.File_id) 
		finishedFiles++
	}

	// TODO: Think of smarter way to check that job is complete. Maybe use a map?
	if finishedFiles == totalFiles {
		finishedFiles = 0
		// Check: Is it necessary to check for lingering jobs. Why would there be any?
		muJobs.Lock()
		if len(currentJobs) > 0 {
			for k := range currentJobs {
				delete(currentJobs, k)
			}
		}
		muJobs.Unlock()
		finishedJobs = make(map[string]struct{})
		changeStatus(false, -1)
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
	ok := changeStatus(true, job.Worker_id)

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
	submitterFailure = make(chan int)

	printl("Got request %v from worker %v", job, job.Worker_id)
	printl("There are %v files", totalFiles)

	// Handout jobs accordingly 
	go func(nextWorker <-chan int, nextJob <- chan FileEntry, finishedAll <-chan int, job JobRequest ) {
		printl("Starting thread to hand out file chunks to workers")
		success := false 
		Loop:
			for {
				select {
				case file := <-nextJob:
					printl("Waiting to send %v", file.Filename)
					select {
					case <- submitterFailure:
						// TODO: delete files on system 
						printl("submitted failed, killing job distribution")
						finishedFiles = 0
						muJobs.Lock()
						if len(currentJobs) > 0 {
							printl("deleting current jobs")
							for k := range currentJobs {
								delete(currentJobs, k)
							}
						}
						muJobs.Unlock()
						changeStatus(false, -1)
						break Loop
					case worker_id := <- nextWorker:
						printl("Worker %v became available", worker_id)
						sendChunk(worker_id, file)
						printl("Sent file %v to worker %v", file.Filename, worker_id)
					}
				case <- finishedAll:
					printl("Finished job, final total: %v", finalTotal)
					os.Remove(job.Filename) 
					success = true
					break Loop 
				}
			}
		if success {
			printl("Finished handing out file chunks")
			sendNotif(job, finalTotal)
		} else{
			printl("submitter failed")
		}	
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
	w_status, err := decodeHeartbeat(req)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := w_status.Id.UID
	if _, ok := currentWorkers[uid]; !ok {
		log.Fatal("Worker expired...reinitialze worker...")
	}

	currentWorkers[uid].Heartbeat <- true 
	ok := checkProgress(w_status)
	// TODO: Handle slow workers more appropriately. 
	if !ok {
		muJobs.Lock()
		slow_job := currentJobs[w_status.Id.UID]
		muJobs.Unlock()
		printl("putting job %v back onto queue", slow_job.file.Filename)
		nextJob <- slow_job.file
		printl("FINISHED PUTTING")
	}
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

func sendChunk(worker_id int, file FileEntry) {
	muJobs.Lock()
	job := WorkerProgress{file, 0, file.Size}
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
func checkProgress(w_status Heartbeat) bool {
	muJobs.Lock()
	w_job, ok:= currentJobs[w_status.Id.UID]
	defer muJobs.Unlock()

	if w_status.Progress == -1 || !ok {
		return true
	} else {
		last_progress := w_job.progress
		curr_progress := w_status.Progress
		currentJobs[w_status.Id.UID].progress = curr_progress
		if curr_progress - last_progress < minProgress {
			printl("current progress %v last progress %v for job %v", curr_progress, last_progress, w_job.file.Filename)
			printl("SLOW WORKER")
			return false
		} else {
			printl("current progress %v last progress %v for job %v", curr_progress, last_progress, w_job.file.Filename)
			printl("WORKER ON TIME")
			return true
		}
	}
}

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
func changeStatus(active bool, worker int) bool {
	muActive.Lock()
	defer muActive.Unlock()
		
	if (active && !activeJob) || !active {
		activeJob = active
		submitter = worker 
		return true
	}
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

func decodeHeartbeat(req *http.Request) (Heartbeat, error) {
    var w_status Heartbeat
    err := json.NewDecoder(req.Body).Decode(&w_status)
	return w_status, err
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

//////// More efficient implentation: split by bytes /////////
// func splitFile(filename string, file_length int) {
// 	fileToBeChunked := "./somebigfile"
// 	 file, err := os.Open(fileToBeChunked)

// 	 if err != nil {
// 			 fmt.Println(err)
// 			 os.Exit(1)
// 	 }

// 	 defer file.Close()

// 	 fileInfo, _ := file.Stat()

// 	 var fileSize int64 = fileInfo.Size()

// 	 const fileChunk = 1 * (1 << 20) // 1 MB, change this to your requirement

// 	 // calculate total number of parts the file will be chunked into

// 	 totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

// 	 fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

// 	 for i := uint64(0); i < totalPartsNum; i++ {

// 			 partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
// 			 partBuffer := make([]byte, partSize)

// 			 file.Read(partBuffer)

// 			 // write to disk
// 			 fileName := "somebigfile_" + strconv.FormatUint(i, 10)
// 			 _, err := os.Create(fileName)

// 			 if err != nil {
// 					 fmt.Println(err)
// 					 os.Exit(1)
// 			 }

// 			 // write/save buffer to disk
// 			 ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

// 			 fmt.Println("Split to : ", fileName)
// 	 }
// }


func splitFile(filename string, file_length int) []FileEntry {    
	var names []FileEntry
	file_input, err := os.Open(filename)
	reader:= bufio.NewReader(file_input)
	file_count := 0 
	line_count := 0

	names = append(names, FileEntry{fmt.Sprintf(filename + "-%d.txt", file_count), "", file_length}) 
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
			names = append(names, FileEntry{fmt.Sprintf("filename-%d.txt", file_count), "", file_length}) 
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
		names[len(names)-1].Size = line_count % file_length
	}
	file_input.Close()

	totalFiles = len(names)
	return names 

 }           

func initializeMaster() {
	currentWorkers = make(map[int]*WorkerState)
	currentJobs = make(map[int]*WorkerProgress)
	finishedJobs = make(map[string]struct{})
	activeJob = false 
	submitter = -1
	finalTotal = 0
	finishedFiles = 0
}

func initializeWorker(conf config.Configuration ) *WorkerState {
	var init_state WorkerState  
	init_state.WorkerId = conf.Id.UID
	init_state.Ip= conf.Id.IP
	init_state.Port = conf.Id.Port
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
							// If dead worker is not the one that submitted 
							if submitter != worker.WorkerId {
								printl("Adding unfinished job %v to queue", w.file.Filename)
								go func() {
									nextJob <- w.file
								} ()
							} else {
								printl("FAILING")
								submitterFailure <- 1
								printl("FINISHED FAILING")
							}
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
	port := ":15213"
	printl("Listening on port %v...", port)
	initializeMaster()

	go sendHeartbeat()

    http.HandleFunc("/job_request", submitRequest)
    http.HandleFunc("/join", join)
	http.HandleFunc("/m_heartbeat", heartbeatHandler) 
	http.HandleFunc("/job_chunk", chunkHandler) 
    http.ListenAndServe(port, nil)
}
