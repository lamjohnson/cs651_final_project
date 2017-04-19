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
)

var worker workerState

type workerState struct {
	Conf	config.Configuration	
	Quit 	chan int 
}

type Job struct {
	Worker_id 	int
	File 		FileEntry
	// File_id 	string
	// File 		string
}

type FileEntry struct {
	Filename 	string
	Data 		string
}

type JobMaster struct {
	Worker_id 	int 
	File_id 	string
	Progress	int
}

type JobRequest struct {
	Filename 	string 
	Worker_id 	int
	Ip			string
	Port 		int
	Total 		int 
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

	time.Sleep(1 * time.Second)
	count := 0 
	for range job.File.Data {
		count += 1
		// time.Sleep(1 * time.Millisecond)
	}
	printl("Got result: %v", count )

	result := JobMaster{job.Worker_id, job.File.Filename , count}
	printl("Sending result back: %v worker %v", result, job.Worker_id)
	sendJobResult(&result)
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
	time.Sleep(10 * time.Millisecond)
	NotifyParty(&worker.Conf)
}


//
// Functions for sending requests to master
//
func JoinParty(conf *config.Configuration) {
	url := "http://127.0.0.1:8080/join"
    b := new(bytes.Buffer)
    json.NewEncoder(b).Encode(conf)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

func NotifyParty(conf *config.Configuration) {
	url := "http://127.0.0.1:8080/m_heartbeat"
    b := new(bytes.Buffer)
    json.NewEncoder(b).Encode(conf)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

func SubmitRequest() {
	url := "http://127.0.0.1:8080/job_request"
    b := new(bytes.Buffer)
	request := JobRequest {"book.txt", worker.Conf.Id.UID, worker.Conf.Party.IP, worker.Conf.Party.Port, 0}
    json.NewEncoder(b).Encode(&request)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

func sendJobResult(result *JobMaster) {
	url := "http://127.0.0.1:8080/job_chunk"
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

func printl(format string, a ...interface{}) {
	fmt.Printf(format + "\n", a...)
}
func makeClient() workerState {
	worker := workerState{}
	worker.Conf = config.Configuration{}
	worker.Quit = make(chan int)

	cfile   := "config2.json"
	worker.Conf.Load(cfile)
	worker.Conf.Id.UID = int(nrand())
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



func main () { 
	worker = makeClient()
	conf := worker.Conf
	ip := conf.Party.IP
	port := conf.Party.Port

	// Already joined a party
	if len(conf.Party.IP) > 0 && conf.Party.Port > 0 && len(conf.Party.Alias) > 0 {
		fmt.Println(conf.Party)
		JoinParty(&conf)
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
			input, _ := reader.ReadString('\n')
			input = strings.TrimRight(input,"\n")

			switch input {
			case "quit":
				break Loop
			case "submit":
				SubmitRequest()
			default:
				printl("Please choose one of the following options: Quit or Submit job")
			}
			
		}
}
