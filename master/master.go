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
    // "strings"
    // "reflect"
)

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

 //    ips := strings.Split("10.0.0.1, 10.0.0.2, 10.0.0.3", ", ")
	// for _, ip := range ips {
	//     fmt.Println(ip)
	// }
}

// /subm
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

    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }

    // Get client's IP address
    _, err = getIP(req)

    if err != nil {
        fmt.Println(err)
    }

    // Receipt for client joining the party
    io.WriteString(w, "Welcome to the party, "+conf.Id.Alias+"\n")


    // Authenticate requester
    // If not in auth list, reject
    // Else, ping back that they were authenticated
    
}

func main() {
	fmt.Println("Listening on port 8080...")
    // http.HandleFunc("/", handler)
    http.HandleFunc("/submitjob", submitJob)
    http.HandleFunc("/join", join)
    http.ListenAndServe(":8080", nil)
}
