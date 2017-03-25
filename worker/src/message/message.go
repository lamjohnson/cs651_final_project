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
	"bytes"
	"os"
	"io"
	"encoding/json"
	// "fmt"
	// "log"
	// "time"
	// "net"
)

// TODO: Should pass url as a parameter
func JoinParty(conf *config.Configuration) {//(bool, error) {
	url := "http://127.0.0.1:8080/join"
    b := new(bytes.Buffer)
    json.NewEncoder(b).Encode(conf)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}

func NotifyParty(conf *config.Configuration) {//(bool, error) {
	url := "http://127.0.0.1:8080/notify"
    b := new(bytes.Buffer)
    json.NewEncoder(b).Encode(conf)
    res, _ := http.Post(url, "application/json; charset=utf-8", b)
    io.Copy(os.Stdout, res.Body)
}
