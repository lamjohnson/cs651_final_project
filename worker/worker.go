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
	// "bufio"
	"os"
	"io"
	// "strings"
	"crypto/rand"
	"math/big"
	"net/http"
	"bytes"
	"encoding/json"
	
)

var worker workerState

type workerState struct {
	conf	config.Configuration	
	quit 	chan int 
}

//
// Functions for handling requests from master
//

// - Work on a file chunk 
// - Process jobs that worker has dependencies for 
func processChunkHandler() {

}

// - Notify app/client job is finished
func finishJobHandler() {

}

// - Ping availability 
// - Ping current progress if any 
// TODO: Check request came from master 
func heartbeatHandler(w http.ResponseWriter, req *http.Request) {
	printl("Received request")	
	time.Sleep(10 * time.Millisecond)
	NotifyParty(&worker.conf)
}


//
// Functions for sending requests to master
//
func sendJob() {
	
}
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


// Generate unique worker IDs 
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
	worker.conf = config.Configuration{}
	worker.quit = make(chan int)

	cfile   := "config.json"
	worker.conf.Load(cfile)
	worker.conf.Id.UID = int(nrand())
	return worker
}

func main () { 
	worker = makeClient()
	conf := worker.conf

	// Already joined a party
	if len(conf.Party.IP) > 0 && conf.Party.Port > 0 && len(conf.Party.Alias) > 0 {
		fmt.Println(conf.Party)
		JoinParty(&conf)

		// reader := bufio.NewReader(os.Stdin) 
		// for {
		// 	fmt.Print("Quit (y/n): ")
		// 	input, _ := reader.ReadString('\n')
		// 	if strings.TrimRight(input, "\n") == "y" {
		// 		worker.quit <- 1 
		// 		break
		// 	}
		// }
	} else {
		fmt.Println("specify party8888 to join and complete config...")
	}

	http.HandleFunc("/w_heartbeat", heartbeatHandler) 
	http.ListenAndServe(":8081", nil)
}
