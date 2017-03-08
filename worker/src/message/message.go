/*
    job.go
    authors: Justin Chen, Johnson Lam
    
    Handles job requests and communication for worker.

    3.7.17
*/

package message

import (
	"net/http"
	"config"
	// "io/ioutil"
	// "bytes"
	// "fmt"
	// "log"
	// "time"
	// "net"
)

func JoinParty(conf *config.Configuration) {//(bool, error) {
	http.Get("127.0.0.1:8080/join")
}