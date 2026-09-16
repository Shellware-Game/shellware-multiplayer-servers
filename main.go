package main

import (
	"fmt"
	"net/http"

	goaway "github.com/TwiN/go-away"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Error upgrading:", err)
		return
	}
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("Error reading message:", err)
			break
		}

		censored := goaway.Censor(string(message))

		fmt.Println("Received:", censored)

		if err := conn.WriteMessage(websocket.TextMessage, []byte(censored)); err != nil {
			fmt.Println("Error writing message:", err)
			break
		}
	}
}

func main() {
	http.HandleFunc("/ws", wsHandler)

	fmt.Println("WebSocket server started on :8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
