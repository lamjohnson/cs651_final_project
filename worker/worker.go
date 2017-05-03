/*
	worker.go
	authors: Justin Chen, Johnson Lam
	
	Worker interface with party.

	3.7.17
*/

package main

import (
	"config"
	"fmt"
	"time"
	"bufio"
	"os"
	"io"
	"strings"
	"crypto/rand"
	"math/big"
	"net/http"
	"bytes"
	"encoding/json"
	"sync"
)

var worker 		workerState
var muActive 	sync.Mutex

type workerState struct {
	Conf		config.Configuration	
	Quit 		chan int 
	Progress 	int 
	Active 		bool
}

type Job struct {
	Worker_id 	int
	File 		FileEntry
}

type FileEntry struct {
	Filename 	string
	Data 		string
	Size 		int 
}

type JobMaster struct {
	Worker_id 	int 
	File_id 	string
	Result 		int
	Progress 	int 
}

type JobRequest struct {
	Filename 	string 
	Url 		string
	Worker_id 	int
	Ip			string
	Port 		int
	Total 		int 
}

type Heartbeat struct {
	Id 			config.Identity 
	Progress 	int
}

//
// Functions for handling requests from master
//

// - Work on a file chunk 
// - Process jobs that worker has dependencies for 
func processChunkHandler(w http.ResponseWriter, req *http.Request) {
	job, err := decodeJob(req)
	if err != nil {
		http.Error(w, err.Error(), 400) 
		return
	}
	printl("Got job for file %v", job.File.Filename)

	// time.Sleep(2 * time.Second)
	wc := 0
	muActive.Lock()
	worker.Active = true 
	muActive.Unlock()
	lines := strings.Split(job.File.Data, "\n")
	for _, line := range lines {
		words := strings.Fields(line)
		muActive.Lock()
		worker.Progress++
		muActive.Unlock()
		for range words {
			wc++
		}
		time.Sleep(20 * time.Millisecond)
	}
	printl("Got result: %v", wc )
	result := JobMaster{job.Worker_id, job.File.Filename , wc, worker.Progress}
	printl("Sending result back: %v worker %v", result, job.Worker_id)
	worker.sendJobResult(&result)

	muActive.Lock()
	worker.Active = false
	worker.Progress = -1
	muActive.Unlock()
}

// - Notify app/client job is finished
func finishJobHandler(w http.ResponseWriter, req *http.Request) {
	job, err := decodeJobResult(req)
	if err != nil {
		http.Error(w, err.Error(), 400) 
		return
	}
	printl("Finished file %v with result %v", job.Filename, job.Total)
}

// - Ping availability 
// - Ping current progress if any 
// TODO: Check request came from master 
func heartbeatHandler(w http.ResponseWriter, req *http.Request) {
	muActive.Lock()
	progress := -1
	if worker.Active {
		progress = worker.Progress
	}
	muActive.Unlock()
	worker.NotifyParty(progress)
}


//
// Functions for sending requests to master
//
func (worker workerState) JoinParty() {
	url := getUrl(worker.Conf.Party.IP, worker.Conf.Party.Port, "join")
    b := new(bytes.Buffer)
    json.NewEncoder(b).Encode(&worker.Conf)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

func (worker workerState) NotifyParty(progress int) {
	url := getUrl(worker.Conf.Party.IP, worker.Conf.Party.Port, "m_heartbeat")
	// url := "http://127.0.0.1:8080/m_heartbeat"
    b := new(bytes.Buffer)
	status := Heartbeat {worker.Conf.Id, progress}
    json.NewEncoder(b).Encode(&status)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

func (worker workerState) SubmitRequest(filename string, text_url string) {
	url := getUrl(worker.Conf.Party.IP, worker.Conf.Party.Port, "job_request")
	// url := "http://127.0.0.1:8080/job_request"
    b := new(bytes.Buffer)
	request := JobRequest {filename, text_url, worker.Conf.Id.UID, worker.Conf.Id.IP, worker.Conf.Id.Port, 0}
    json.NewEncoder(b).Encode(&request)
    res, err := http.Post(url, "application/json; charset=utf-8", b)
	if err != nil {
		printl("Network error, please try submitting again")
		return
	}
    io.Copy(os.Stdout, res.Body)
}

func (worker workerState) sendJobResult(result *JobMaster) {
	url := getUrl(worker.Conf.Party.IP, worker.Conf.Party.Port, "job_chunk")
	// url := "http://127.0.0.1:8080/job_chunk"
    b := new(bytes.Buffer)
    json.NewEncoder(b).Encode(result)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

//
// Internal Functions 
//
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func getUrl(base string, port int, endpoint string) string {
	url := fmt.Sprintf("http://%v:%v/%v", base, port, endpoint)
	return url 
}

func printl(format string, a ...interface{}) {
	fmt.Printf(format + "\n", a...)
}
func makeClient() workerState {
	worker := workerState{}
	worker.Conf = config.Configuration{}
	worker.Quit = make(chan int)
	worker.Progress = -1
	worker.Active = false

	cfile   := "local.json"
	// cfile   := "config.json"
	worker.Conf.Load(cfile)
	worker.Conf.Id.UID = int(nrand())
	printl("Worker state %v", worker.Conf.Id.Port)
	return worker
}

func decodeJobResult(req *http.Request) (JobRequest, error) {
	var job JobRequest
	err := json.NewDecoder(req.Body).Decode(&job)
	return job, err 
}

func decodeJob(req *http.Request) (Job, error) {
	var job Job
	err := json.NewDecoder(req.Body).Decode(&job)
	return job, err 
}

func readUserInput(reader *bufio.Reader) string {
	input, _ := reader.ReadString('\n')
	input = strings.TrimRight(input,"\n")
	return input
}


func main () { 
	worker = makeClient()
	conf := worker.Conf
	ip := conf.Id.IP
	port := conf.Id.Port

	// Already joined a party
	if len(conf.Party.IP) > 0 && conf.Party.Port > 0 && len(conf.Party.Alias) > 0 {
		fmt.Println(conf.Party)
		worker.JoinParty()
	} else {
		fmt.Println("specify party to join and complete config...")
	}

	http.HandleFunc("/w_heartbeat", heartbeatHandler) 
	http.HandleFunc("/process_chunk", processChunkHandler)
	http.HandleFunc("/finish_job", finishJobHandler)
	printl("listening on ip %v port %v", ip, port)
	listen_port := fmt.Sprintf(":%v", port)
	go http.ListenAndServe(listen_port, nil)

	reader := bufio.NewReader(os.Stdin) 
	Loop:
		for {
			fmt.Print("Hi, what would you like to do?\n")
			input := readUserInput(reader)

			switch input {
			case "quit":
				break Loop
			case "submit":
				fmt.Print("Please specify a URL\n")
				url := readUserInput(reader)
				fmt.Print("Please specify a filename\n")
				filename := readUserInput(reader)
				worker.SubmitRequest(filename, url)
			default:
				printl("Please choose one of the following options: Quit or Submit job")
			}
			
		}
}
