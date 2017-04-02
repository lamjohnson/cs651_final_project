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
    // "strings"
    // "reflect"
)

type ServerState struct {
	timeout		*time.Timer
	resetTimer 	chan int 
	active		bool
}

var servers [10]ServerState
var expire chan int 

// https://blog.golang.org/context/userip/userip.go
func getIP(req *http.Request) (net.IP, error) {
    ip, _, err := net.SplitHostPort(req.RemoteAddr)
    if err != nil {
        return nil, fmt.Errorf("userip: %q is not IP:port", req.RemoteAddr)
    }

    userIP := net.ParseIP(ip)

    if userIP == nil {
        return nil, fmt.Errorf("userip: %q is not IP:port", req.RemoteAddr)
    } else {
        fmt.Println("ip: ",string(ip))
    }
    return userIP, nil
}

// Not currently used
func handler(w http.ResponseWriter, req *http.Request) {
    fmt.Fprintf(w, "Master up and running!", req.URL.Path[1:])
    // ip, _ := getIP(req)

    req.Header.Get("x-forwarded-for")
}

func submitJob(w http.ResponseWriter, req *http.Request) {
    params := req.URL.Query()
    for k, v := range params {
        fmt.Println(k, " ", v)
    }
}

func join(w http.ResponseWriter, req *http.Request) {
    // Decode configuration passed as a parameter
    var conf config.Configuration
    err := json.NewDecoder(req.Body).Decode(&conf)
	fmt.Printf("Worker %v joining master\n", conf.Id.UID)

    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }

    // Get client's IP address
    _, err = getIP(req)

    if err != nil {
		fmt.Println(err)
    }

	// Initialize heartbeat timer for client 
	uid := conf.Id.UID
	servers[uid].timeout = setTimer(false, 1, servers[uid].timeout)

	// when timeout expires, notify master of this client 
	go func(uid int, timeout *time.Timer) {
		for {
			select {
			case <- timeout.C:
				expire <- uid
			}
		}
	}(uid, servers[uid].timeout)


    // Receipt for client joining the party
    io.WriteString(w, "Welcome to the party, "+conf.Id.Alias+"\n")


    // Authenticate requester
    // If not in auth list, reject
    // Else, ping back that they were authenticated
    
}

func notify(w http.ResponseWriter, req *http.Request) {
    // Decode configuration passed as a parameter
    var conf config.Configuration
    err := json.NewDecoder(req.Body).Decode(&conf)
	fmt.Printf("Worker %v heartbeat\n", conf.Id.UID)

    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }

    // Get client's IP address
    _, err = getIP(req)

    if err != nil {
        fmt.Println(err)
    }

	// Reset heartbeat timer for client 
	uid := conf.Id.UID
	servers[uid].timeout = setTimer(true, 1, servers[uid].timeout)

    // Receipt for client joining the party
    // io.WriteString(w, "Notified the party, "+conf.Id.Alias+"\n")
}

func setTimer(stop bool, length int, timeout *time.Timer) *time.Timer {
	if stop {
		timeout.Stop()
		timeout.Reset(time.Duration(length)* time.Second)
	} else {
		timeout = time.NewTimer(time.Duration(length)* time.Second)
	}
	return timeout
}


func heartbeat() {
	for {
		select {
		case server_id := <- expire:
			fmt.Printf("Timer expired on worker %v\n", server_id)
		} 
	}
}

func initialize() {
	expire = make(chan int) 
	// resetTimer = make(chan int) 
}

func main() {
	fmt.Println("Listening on port 8080...")
	initialize()
	go heartbeat()

    // http.HandleFunc("/", handler)
    http.HandleFunc("/submitjob", submitJob)
    http.HandleFunc("/join", join)
	http.HandleFunc("/notify", notify) 
    http.ListenAndServe(":8080", nil)
}
