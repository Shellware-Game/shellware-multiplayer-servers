package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"unsafe"

	"github.com/gorilla/websocket" //websockets :drool-emoji:
	"github.com/xiaonanln/goworld/engine/common"
	"github.com/xiaonanln/goworld/engine/netutil"
	"github.com/xiaonanln/goworld/engine/proto"
)

const (
	MT_SET_CLIENT_CLIENTID           proto.MsgType = 2001
	MT_SYNC_POSITION_YAW_FROM_CLIENT proto.MsgType = 47
	MT_SYNC_POSITION_YAW_ON_CLIENTS  proto.MsgType = 1518
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	id        common.ClientID
	conn      *websocket.Conn
	connected bool
	position  [3]float32
	yaw       float32
	server    *Server
	sendMu    sync.Mutex
}

type Server struct {
	clients   map[common.ClientID]*Client
	clientsMu sync.RWMutex
	nextID    int
	authority common.ClientID
	mux       *http.ServeMux
}

func NewServer() *Server {
	return &Server{
		clients: make(map[common.ClientID]*Client),
		mux:     http.NewServeMux(),
	}
}

func (s *Server) Run(addr string) error {
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	s.mux.HandleFunc("/ws", s.handleWS)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	log.Printf("Goworld websocket server listening on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade error: %v", err)
		return
	}

	id := common.ClientID(common.GenClientID())
	s.nextID++

	isAuthority := s.authority.IsNil()
	if isAuthority {
		s.authority = id
	}

	client := &Client{
		id:        id,
		conn:      conn,
		connected: true,
		server:    s,
	}

	s.clientsMu.Lock()
	s.clients[id] = client
	s.clientsMu.Unlock()

	log.Printf("Client %s connected (authority=%v) (%d total)", id, isAuthority, s.clientCount())

	s.sendWelcome(client, isAuthority)

	for client.connected {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("recv error from %s: %v", id, err)
			}
			break
		}

		if len(message) < 6 {
			log.Printf("message too short from %s: %d bytes", id, len(message))
			continue
		}

		msgType := proto.MsgType(binary.LittleEndian.Uint16(message[4:6]))
		body := message[6:]

		switch msgType {
		case MT_SYNC_POSITION_YAW_FROM_CLIENT:
			if len(body) < common.ENTITYID_LENGTH+16 {
				continue
			}
			_ = common.EntityID(body[:common.ENTITYID_LENGTH])
			x := readFloat32(body[common.ENTITYID_LENGTH:])
			y := readFloat32(body[common.ENTITYID_LENGTH+4:])
			z := readFloat32(body[common.ENTITYID_LENGTH+8:])
			yaw := readFloat32(body[common.ENTITYID_LENGTH+12:])

			client.position = [3]float32{x, y, z}
			client.yaw = yaw

			s.broadcastPosition(client.id)
		}
	}

	client.connected = false
	conn.Close()

	s.clientsMu.Lock()
	delete(s.clients, id)
	s.clientsMu.Unlock()

	log.Printf("Client %s disconnected (%d total)", id, s.clientCount())
}

func (s *Server) sendWelcome(c *Client, isAuthority bool) {
	pkt := netutil.NewPacket()
	pkt.AppendUint16(uint16(MT_SET_CLIENT_CLIENTID))
	pkt.AppendClientID(c.id)
	pkt.AppendBool(isAuthority)
	data := packetWireBytes(pkt)
	pkt.Release()
	c.sendBytes(data)
}

func (s *Server) broadcastPosition(senderID common.ClientID) {
	s.clientsMu.RLock()
	type clientEntry struct {
		id       common.ClientID
		position [3]float32
		yaw      float32
	}
	clients := make([]clientEntry, 0, len(s.clients))
	for id, c := range s.clients {
		clients = append(clients, clientEntry{
			id:       id,
			position: c.position,
			yaw:      c.yaw,
		})
	}
	s.clientsMu.RUnlock()

	pkt := netutil.NewPacket()
	pkt.AppendUint16(uint16(MT_SYNC_POSITION_YAW_ON_CLIENTS))
	pkt.AppendUint32(uint32(len(clients)))
	for _, other := range clients {
		pkt.AppendClientID(other.id)
		pkt.AppendFloat32(other.position[0])
		pkt.AppendFloat32(other.position[1])
		pkt.AppendFloat32(other.position[2])
		pkt.AppendFloat32(other.yaw)
	}
	data := packetWireBytes(pkt)
	pkt.Release()

	s.clientsMu.RLock()
	for _, c := range s.clients {
		c.sendBytes(data)
	}
	s.clientsMu.RUnlock()
}

func (s *Server) clientCount() int {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	return len(s.clients)
}

func (c *Client) sendBytes(data []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}

func packetWireBytes(pkt *netutil.Packet) []byte {
	plen := pkt.GetPayloadLen()
	payload := pkt.Payload()
	out := make([]byte, 4+plen)
	binary.LittleEndian.PutUint32(out[:4], plen)
	copy(out[4:], payload)
	return out
}

func readFloat32(b []byte) float32 {
	bits := binary.LittleEndian.Uint32(b)
	return *(*float32)(unsafe.Pointer(&bits))
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "15001"
	}
	srv := NewServer()
	log.Fatal(srv.Run(":" + port))
}
