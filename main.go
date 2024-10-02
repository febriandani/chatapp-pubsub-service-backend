// pubsub-service-backend/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type PubSub struct {
	subscribers map[string][]chan string
	mutex       sync.Mutex
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string][]chan string),
	}
}

// Publish a message to a specific topic
func (ps *PubSub) Publish(topic, msg string) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if subs, found := ps.subscribers[topic]; found {
		for _, sub := range subs {
			go func(ch chan string) {
				ch <- msg
			}(sub)
		}
	}
}

// Subscribe to a topic
func (ps *PubSub) Subscribe(topic string) chan string {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ch := make(chan string)
	ps.subscribers[topic] = append(ps.subscribers[topic], ch)
	return ch
}

var ps = NewPubSub()

// Handler for publishing messages
func publishHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic   string `json:"topic"`
		Message string `json:"message"`
	}

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	ps.Publish(req.Topic, req.Message)
	w.Write([]byte("Message published successfully"))
}

// Handler for subscribing to a topic
func subscribeHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Missing topic", http.StatusBadRequest)
		return
	}

	ch := ps.Subscribe(topic)

	// Stream the messages as they come in
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for msg := range ch {
		fmt.Fprintf(w, "data: %s\n\n", msg)
		w.(http.Flusher).Flush() // Flush data to client immediately
	}
}

func main() {
	http.HandleFunc("/publish", publishHandler)
	http.HandleFunc("/subscribe", subscribeHandler)

	log.Println("PubSub service running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
