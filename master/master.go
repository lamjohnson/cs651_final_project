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
    // "strings"
    // "reflect"
)

type ServerState struct {
	timeout		*time.Timer
	resetTimer 	chan int 
	active		bool
}

var expiredServers 	chan int 
var servers			map[int]*ServerState
var activeWork		bool 
var muServers		sync.Mutex
var muJobs			sync.Mutex

const create = "create"
const reset	 = "reset"
const close  = "close"

func finishChunk(w http.ResponseWriter, req *http.Request) {
	// pass 
}

// look at mapreduce code 
func submitJob(w http.ResponseWriter, req *http.Request) {
	muJobs.Lock() 
	avail_workers := currentServers()
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

func join(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeRequest(req) 
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID
	
	if _, ok := servers[uid]; ok {
		log.Fatal("Intiailzing a worker that exists...")
	}

	fmt.Printf("Worker %v joining master\n", conf.Id.UID)

	// Initialize worker mapping  
	initializeWorker(uid)
	servers[uid].timeout = setTimer(create, 1, servers[uid].timeout)

	// go routine to notify master when client heartbeat expires 
	go func(server_id int, timeout *time.Timer) {
		for {
			select {
			case <- timeout.C:
				fmt.Printf("Deleting worker from mapping\n")
				delete(servers, server_id)
				expiredServers <- uid
			}
		}
	}(uid, servers[uid].timeout)

    // Receipt for client joining the party
    io.WriteString(w, "Welcome to the party, "+conf.Id.Alias+"\n")
}

func incomingHeartbeat(w http.ResponseWriter, req *http.Request) {
	conf, err := decodeRequest(req)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
	uid := conf.Id.UID

	if _, ok := servers[uid]; !ok {
		log.Fatal("Worker expired...reinitialze worker...")
	}

	// Reset heartbeat timer for worker 
	servers[uid].timeout = setTimer(reset, 1, servers[uid].timeout)

}

func currentServers() [] int {
	muServers.Lock()
	servers := make([]int, len(servers))
	muServers.Unlock()
	i := 0 
	for key := range servers {
		servers[i] = key 
		i++
	}
	return servers 
}

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


func notifyExpired() {
	for {
		select {
		case server_id := <- expiredServers:
			fmt.Printf("Timer expired on worker %v\n", server_id)
		} 
	}
}

func initializeMaster() {
	servers = make(map[int]*ServerState)
	expiredServers = make(chan int) 
	activeWork = false 
}

func initializeWorker(worker_id int) {
	var init_state ServerState  
	servers[worker_id] = &init_state
}

func main() {
	fmt.Println("Listening on port 8080...")
	initializeMaster()
	go notifyExpired()

    http.HandleFunc("/submitjob", submitJob)
    http.HandleFunc("/join", join)
	http.HandleFunc("/heartbeat", incomingHeartbeat) 
    http.ListenAndServe(":8080", nil)
}
