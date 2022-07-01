package state

import (
	"time"
)

type MessageID = int

type Message struct {
	id MessageID
	content interface{}
}

const chanSize int = 100

type EventHandler map[MessageID]func(interface{})

type EventRepository struct {
	evhl EventHandler
}

type TimerID = int
type TimeoutHandler map[TimerID]func(int)

const (
	ConstInterval  = 0x01
	RandomInterval = 0x02
	EnableTimer    = 0x04
)

type TimeoutInfo struct {
	id TimerID
	flags int
	initialInterval time.Duration
	tmr *time.Timer
	handler func(int)
}

type TimeoutRepository struct {
	tohl []TimeoutInfo
}

type StateMachine struct {
	ch      chan Message
	evRepo EventRepository
	tmRepo TimeoutRepository
}

func (m *StateMachine) RegisterEventHandler(id MessageID, handler func(interface{})) {
	m.evRepo.evhl[id] = handler
}

func (m *StateMachine) ExecuteEventHandler(id MessageID, param interface{}) bool {
	h, ok := m.evRepo.evhl[id]
	if ok {
		h(param)
	}

	return true
}

func (m *StateMachine)RegisterTimeoutHandler(id TimerID, flags int, interval time.Duration, handler func(int)) {
	m.tmRepo.tohl = append(m.tmRepo.tohl, TimeoutInfo{id, flags, interval, nil, handler})
}

func (m *StateMachine)ExecuteTimeoutHandler()  {
	for _, h := range m.tmRepo.tohl {
		if h.flags & EnableTimer > 0 {
			if h.tmr == nil {
				if h.flags & RandomInterval > 0{
					//h.tmr = time.NewTimer()
				} else if h.flags & ConstInterval > 0{
					h.tmr = time.NewTimer(h.initialInterval)
				}
			}
		} else {
			if h.tmr != nil {
				h.tmr.Stop()
			}

			break
		}

		select {
		case <- h.tmr.C:
			h.handler(h.id)

			if h.flags == RandomInterval {

			} else if h.flags == ConstInterval {
				h.tmr.Reset(h.initialInterval)
			}

			break
		}
	}
}

func (m *StateMachine) Run() bool {
	m.ch = make(chan Message, chanSize)
	m.evRepo.evhl = make(EventHandler)

	var msg Message
	for {
		select {
		case msg = <- m.ch:
			{
				if !m.ExecuteEventHandler(msg.id, msg.content) {
					return false
				}
			}

			break
		default:

		}

		m.ExecuteTimeoutHandler()
	}


	return true
}
