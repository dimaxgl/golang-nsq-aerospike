package heap

import (
	"github.com/nsqio/go-nsq"
	"log"
)

type Heap struct {
	maxCount int
	count    int
	stack    []*nsq.Message
	c        chan bool
}

func NewHeap(maxCount int) *Heap {
	c := make(chan bool, maxCount)
	s := make([]*nsq.Message, 0)
	return &Heap{maxCount:maxCount, c:c, stack:s}
}

func (h *Heap) Add(msg *nsq.Message) {
	h.c <- true
	h.stack = append(h.stack, msg)
}

func (h *Heap) Get() []*nsq.Message {
	defer func() { h.c = make(chan bool, h.maxCount) }()
	log.Println(len(h.stack))
	return h.stack
}
