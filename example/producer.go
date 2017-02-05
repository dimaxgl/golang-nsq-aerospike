package main

import (
	"github.com/nsqio/go-nsq"
	log "github.com/Sirupsen/logrus"
	"encoding/json"
	"flag"
)

var (
	nsqConnFlag = flag.String(`nsq`, ``, `NSQ connection string`)
)

func main() {
	flag.Parse()
	config := nsq.NewConfig()
	p, err := nsq.NewProducer(*nsqConnFlag, config)
	for i := 0; i < 10000; i++ {
		as := Link{
			A1:      i,
			A2:      `qqqq`,
		}
		j, _ := json.Marshal(as)
		err = p.Publish("test", j)
		if err != nil {
			log.Fatal("Could not connect")
		}
	}

	p.Stop()
}

type Link struct {
	A1 int `json:"a1"`
	A2 string   `json:"a2"`
}
