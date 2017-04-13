/*
    master.go
    authors: Justin Chen, Johnson Lam
    
    Master interface to party.

    3.7.17
*/

package main

import (
    "fmt"
    "net/http"
    "net"
    "io"
    "config"
    "encoding/json"
	"time" 
	"log"
	"sync"
	"crypto/rand"
	"math/big"
)

var expiredServers 	chan int 
var servers			map[int]*WorkerState
var activeWork		bool 
var muServers		sync.Mutex
var muJobs			sync.Mutex

const create = "create"
const reset	 = "reset"
const close  = "close"

type WorkerState struct {
	workerId	int
	url 		string 
	timeout		*time.Timer
	heartbeat 	chan bool
	fail 		int 
	active		bool
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

// - Mark chunk of file as finished, send next chunk 
func finishChunk(w http.ResponseWriter, req *http.Request) {
	// pass 
}

// look at mapreduce code 
// - No need to specify work type, assume word count 
//     - Later on specify type of work to be done 
// - Include file or filename url to retreive 
// - Divide up file into chunks and send to workers 
func submitJob(w http.ResponseWriter, req *http.Request) {
	muJobs.Lock() 
	// avail_workers := currentServers()
	for {
		// hand out job to available worker 
		select {
		//case worker becomes available
			// check for next chunk to send 
		//case timer on worker expired while doing work 
			// send work to another worker 
		}
	}
	muJobs.Unlock()
    // params := req.URL.Query()
    // for k, v := range params {
    //     fmt.Println(k, " ", v)
    // }
}

// - Check availability of worker 
// - Check progress of workers (github.com/cheggaaa/pb)
func heartbeatHandler(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeRequest(req)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID
	if _, ok := servers[uid]; !ok {
		log.Fatal("Worker expired...reinitialze worker...")
	}
	servers[uid].heartbeat <- true 
}

// - Config: worker machine includes available dependencies 
func join(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeRequest(req) 
	ip, _ := getIP(req)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID
	if _, ok := servers[uid]; ok {
		log.Fatal("Intiailzing a worker that exists...")
	}

	fmt.Printf("Worker %v joining master\n", conf.Id.Alias)
	initializeWorker(uid, ip.String())

    // Receipt for client joining the party
    io.WriteString(w, "Welcome to the party, "+conf.Id.Alias+"\n")
}
//
// Functions for sending requests to worker 
//
func sendHeartbeat() {
	for {
		muServers.Lock()
		for worker := range servers {
			go func(worker *WorkerState) {
				printl("Pinging worker %v at ip %v", worker.workerId, worker.url)
				url := "http://127.0.0.1:8081/w_heartbeat"
				http.Get(url)
			} (servers[worker])
		}
		muServers.Unlock()
		time.Sleep(3 * time.Second)
	}
}

// func currentServers() [] int {
// 	muServers.Lock()
// 	servers := make([]int, len(servers))
// 	muServers.Unlock()
// 	i := 0 
// 	for key := range servers {
// 		servers[i] = key 
// 	worker.	i++
// 	}
// 	return servers 
// }


//
// Internal Functions 
//

// https://blog.golang.org/context/userip/userip.go
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

func decodeRequest(req *http.Request) (config.Configuration, error) {
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
	servers = make(map[int]*WorkerState)
	expiredServers = make(chan int) 
	activeWork = false 
}

func initializeWorker(worker_id int, ip string) *WorkerState {
	var init_state WorkerState  
	init_state.workerId = worker_id
	init_state.url = ip 
	init_state.heartbeat = make(chan bool)
	init_state.fail = 0 
	init_state.active = true
	
	servers[worker_id] = &init_state
	servers[worker_id].timeout = setTimer(create, 5, init_state.timeout)

	go func(server_id int, worker *WorkerState) {
		Loop:
			for {
				select {
				case <- worker.timeout.C:
					if worker.fail < 3 {
						printl("Failed to hear from worker %v %v time(s)", worker.workerId, worker.fail)
						worker.timeout = setTimer(reset, 5, worker.timeout) 
						worker.fail++
					} else {
						fmt.Printf("Deleting worker from active workers list\n")
						muServers.Lock()
						delete(servers, server_id)
						muServers.Unlock()

						expiredServers <- server_id
						break Loop
					}
				case <- worker.heartbeat:
					worker.timeout = setTimer(reset, 5, worker.timeout) 
					worker.fail = 0 
				}
			}
	}(worker_id, &init_state)

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

    http.HandleFunc("/submitjob", submitJob)
    http.HandleFunc("/join", join)
	http.HandleFunc("/m_heartbeat", heartbeatHandler) 
    http.ListenAndServe(":8080", nil)
}
