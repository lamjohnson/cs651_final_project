/*
	config.go
	authors: Justin Chen, Johnson Lam
	
	Worker interface with party.

	3.7.17
*/

package config

import (
	"time"
	"encoding/json"
	"io/ioutil"
	"os"
	"log"
	"strconv"
)

type Configuration struct {
	Party    Party 		   `json:"party"`
	Id       Identity 	   `json:"id"`
	Space    Storage  	   `json:"storage"`
	Memory   RAM 	  	   `json:"ram"`
	Proc     CPU      	   `json:"cpu"`
	GProc    GPU      	   `json:"gpu"`
	Net      Network  	   `json:"network"`
	Sys    	 System   	   `json:"sys"`
	Duration time.Duration `json:"duration"`
	URL      string
}

// Loads the saved configuration
// Loads config file from /src/config
func (c *Configuration) Load(conf string) {
	cwd, err := os.Getwd()

	if _, err := os.Stat(cwd+"/src/config/"+conf); os.IsNotExist(err) {
		log.Fatal("could not load "+conf)
	}
	raw, err := ioutil.ReadFile("./src/config/"+conf)

	if err != nil {
		log.Fatal("couldn't read config...fuck you!")
	}

    json.Unmarshal(raw, c)

    c.URL = c.Party.IP+":"+strconv.Itoa(c.Party.Port)
    // fmt.Printf("Results: %+v\n", jsontype)
}

type Party struct {
	IP 	  string `json:"ip"`
	Port  int    `json:"port"`
	Alias string `json:"alias"`
}

type Identity struct {
	IP 	  string `json:"ip"`
	Alias string `json:"alias"`
}

type Storage struct {
	Avail int `json:"avail"`
	Mem   int `json:"mem"`
}

type RAM struct {
	Avail int `json:"avail"`
	Mem   int `json:"mem"`
}

type CPU struct {
	Avail int    `json:"avail"`
	GHZ   string `json:"ghz"`
	Model string `json:"model"`
}

type GPU struct {
	Avail int 	 `json:"avail"`
	Mem   int 	 `json:"mem"`
	Ram   RAM    `json:"ram"`
	Model string `json:"model"`
}

type Network struct {
	Bandwidth int `json:"bandwidth"`
}

type System struct {
	OS 	   string `json:"os"`
	Verion string `json:"version"`
}

