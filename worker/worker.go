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
	"message"
	"time"
	"bufio"
	"os"
	"strings"
)

type workerArgs struct {
	quit chan int 
}

// create instances of clients 
func makeClient() {

}

func main () { 
	// Create config
	var conf = config.Configuration{}
	worker := workerArgs{}
	worker.quit = make(chan int)

	cfile   := "config.json"
	conf.Load(cfile)

	// Already joined a party
	if len(conf.Party.IP) > 0 && conf.Party.Port > 0 && len(conf.Party.Alias) > 0 {
		fmt.Println(conf.Party)
		message.JoinParty(&conf)
		go sendHeartBeat(&worker, &conf)

		reader := bufio.NewReader(os.Stdin) 
		for {
			fmt.Print("Quit (y/n): ")
			input, _ := reader.ReadString('\n')
			if strings.TrimRight(input, "\n") == "y" {
				worker.quit <- 1 
				break
			}
		}
	} else {
		fmt.Println("specify party to join and complete config...")
	}
}

//send regular heartbeats every 500ms 
func sendHeartBeat(worker *workerArgs, conf *config.Configuration) {
	Loop:
		for {
			select {
			case <- worker.quit:
				fmt.Printf("Client exiting...")
				break Loop
			default:
				time.Sleep(500 * time.Millisecond)
				message.NotifyParty(conf)
			}
		}
}
