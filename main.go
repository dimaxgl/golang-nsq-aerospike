package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
	"os"
	"sync"
	"errors"
	"os/signal"
	"syscall"
	"encoding/json"
	"time"
	"github.com/aerospike/aerospike-client-go"
	"math/rand"
	"fmt"
	"github.com/dimaxgl/golang-nsq-aerospike/heap"
)

var (
	topicFlag        = flag.String(`topic`, ``, `Input NSQ topic`)
	nsqConnFlag      = flag.String(`nsq`, ``, `NSQ connection string`)
	messageCountFlag = flag.Int(`message-count`, 1000, `Consumers count`)
	secFlag          = flag.Int(`sec`, 5, `Seconds interval`)

	aeroSpikeHost = flag.String(`aerospike-server`, ``, `Aerospike server address`)
	aeroSpikePort = flag.Int(`aerospike-port`, 0, `Aerospike server port`)
)

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

type MessageContainer struct {
	Message *nsq.Message
	Data    aerospike.BinMap
}

func main() {
	log.SetFormatter(&log.TextFormatter{ForceColors:true, FullTimestamp:true})
	flag.Parse()

	var err error
	if err = validateFlags(); err != nil {
		log.Fatal(err)
	}

	var (
		wg       sync.WaitGroup
		config   *nsq.Config
		cons     *nsq.Consumer
		ac       *aerospike.Client
		TermChan = make(chan bool)
		//SaveChan = make(chan *nsq.Message)
		h *heap.Heap
	)

	h = heap.NewHeap(*messageCountFlag)

	if ac, err = aerospike.NewClient(*aeroSpikeHost, *aeroSpikePort); err != nil {
		log.Fatal(`Aerospike connect err: `, err)
	} else {
		log.Infoln(`Connected to Aerospike`)
		defer ac.Close()
	}

	config = nsq.NewConfig()

	if cons, err = nsq.NewConsumer(*topicFlag, `sample_channel`, config); err != nil {
		log.Fatal(`NSQ consumer err: `, err)
	}

	cons.AddHandler(GetNSQLHandlerFunc(h))

	if err = cons.ConnectToNSQD(*nsqConnFlag); err != nil {
		log.Fatal(`NSQ connect err: `, err)
	}

	defer func() {
		if err := cons.DisconnectFromNSQD(*nsqConnFlag); err != nil {
			log.Fatal(err)
		}
		log.Infoln(`Disconnected from NSQ`)
	}()

	wg.Add(1)
	go listenSystemSignals(&wg, TermChan)

	wg.Add(1)
	go dataSaver(&wg, ac, *secFlag, TermChan, h)

	wg.Wait()

}

func validateFlags() error {
	if *topicFlag == `` {
		return errors.New("Topic must be defined")
	}
	if *nsqConnFlag == `` {
		return errors.New(`NSQ conn must be defined`)
	}
	if *secFlag < 1 {
		return errors.New(`Value of seconds must be greater than zero`)
	}
	if *messageCountFlag < 1 {
		return errors.New(`Value of messages must be greater than zero`)
	}

	if *aeroSpikeHost == `` {
		return errors.New(`Aerospike server host must be defined`)
	}

	if *aeroSpikePort < 1 {
		return errors.New(`Aerospike server host must be defined`)
	}
	return nil
}

func listenSystemSignals(wg *sync.WaitGroup, tc chan bool) {
	defer wg.Done()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt)
	log.Info(`Stopped by signal: `, <-ch)
	tc <- true
}

func GetNSQLHandlerFunc(heap *heap.Heap) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		heap.Add(message)
		return nil
	})
}

func dataSaver(wg *sync.WaitGroup, ac *aerospike.Client, durationSec int, tc chan bool, h *heap.Heap) {
	defer wg.Done()
	t := time.NewTicker(time.Duration(durationSec) * time.Second)
	saveData := make([]MessageContainer, 0)
	for {
		select {
		case <-tc:
			log.Infoln(`Data processor is closed`)
			return
		case <-t.C:
			data := h.Get()
			l := len(data)
			if l > 0 {
				for _, d := range data {
					var mc MessageContainer
					if err := json.Unmarshal(d.Body, &mc.Data); err != nil {
						log.Warnln(`Incorrect NSQ message: `, err)
					}
					mc.Message = d
					saveData = append(saveData, mc)
					d.Touch()
				}
				log.Infoln(`Time to save messages: `, l)
				if err := saveBatch(ac, saveData); err != nil {
					log.Warnln(`Failed to save data to Aerospike: `, err)
				} else {
					log.Infoln(`Saved to Aerospike`)
					saveData = make([]MessageContainer, 0)
				}
			} else {
				log.Infoln(`Nothing to save`)
			}

		}

	}
}

func saveBatch(ac *aerospike.Client, data []MessageContainer) error {

	for _, mc := range data {
		key, err := aerospike.NewKey(`test`, RandStringBytesMaskImprSrc(5), `key`)
		if err != nil {
			return fmt.Errorf("Aerospike key error: %s", err)
		}

		if err := ac.Put(nil, key, mc.Data); err != nil {
			return fmt.Errorf("Aerospike put error: %s", err)
		} else {
			mc.Message.Finish()
		}
	}
	return nil
}

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n - 1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
