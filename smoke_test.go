package main

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xiaonanln/goworld/engine/common"
	"github.com/xiaonanln/goworld/engine/netutil"
	"github.com/xiaonanln/goworld/engine/proto"
)

func dialWS(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	return conn
}

func recvWS(t *testing.T, conn *websocket.Conn) (proto.MsgType, []byte) {
	t.Helper()
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("recv ws: %v", err)
	}
	if len(message) < 6 {
		t.Fatalf("message too short: %d bytes", len(message))
	}
	msgType := proto.MsgType(binary.LittleEndian.Uint16(message[4:6]))
	return msgType, message[6:]
}

func TestServerSendsWelcome(t *testing.T) {
	srv := NewServer()
	go srv.Run(":15099")
	time.Sleep(200 * time.Millisecond)

	conn := dialWS(t, "localhost:15099")
	defer conn.Close()

	msgType, body := recvWS(t, conn)
	if msgType != MT_SET_CLIENT_CLIENTID {
		t.Fatalf("expected welcome msgtype %d, got %d", MT_SET_CLIENT_CLIENTID, msgType)
	}
	id := common.ClientID(body[:common.CLIENTID_LENGTH])
	auth := body[common.CLIENTID_LENGTH] != 0
	if len(id) != common.CLIENTID_LENGTH {
		t.Fatalf("client id length %d, want %d", len(id), common.CLIENTID_LENGTH)
	}
	if !auth {
		t.Fatalf("first client should be authority")
	}
	fmt.Printf("smoke: got welcome id=%s auth=%v\n", id, auth)
}

func TestServerBroadcastsPosition(t *testing.T) {
	srv := NewServer()
	go srv.Run(":15100")
	time.Sleep(200 * time.Millisecond)

	var c1, c2 *websocket.Conn

	c1 = dialWS(t, "localhost:15100")
	defer c1.Close()
	recvWS(t, c1) // welcome

	c2 = dialWS(t, "localhost:15100")
	defer c2.Close()
	msgType, body := recvWS(t, c2)
	c2ID := common.ClientID(body[:common.CLIENTID_LENGTH])
	_ = msgType
	_ = body[common.CLIENTID_LENGTH]

	// client 1 sends a position update
	pkt := netutil.NewPacket()
	pkt.AppendUint16(uint16(MT_SYNC_POSITION_YAW_FROM_CLIENT))
	pkt.AppendEntityID(common.EntityID("0000000000000000"))
	pkt.AppendFloat32(1)
	pkt.AppendFloat32(2)
	pkt.AppendFloat32(3)
	pkt.AppendFloat32(0)
	c1.WriteMessage(websocket.BinaryMessage, packetWireBytesForTest(pkt))
	pkt.Release()
	time.Sleep(200 * time.Millisecond)

	msgType, body = recvWS(t, c2)
	if msgType != MT_SYNC_POSITION_YAW_ON_CLIENTS {
		t.Fatalf("expected sync msgtype %d, got %d", MT_SYNC_POSITION_YAW_ON_CLIENTS, msgType)
	}
	count := binary.LittleEndian.Uint32(body[:4])
	if count != 2 {
		t.Fatalf("count = %d, want 2 (all clients)", count)
	}
	off := 4
	for i := uint32(0); i < count; i++ {
		_ = common.ClientID(body[off:])
		off += common.CLIENTID_LENGTH + 16
		fmt.Printf("smoke: c2 entry %d id=%s\n", i, c2ID)
	}
	_ = c1
}

func packetWireBytesForTest(pkt *netutil.Packet) []byte {
	plen := pkt.GetPayloadLen()
	payload := pkt.Payload()
	out := make([]byte, 4+plen)
	binary.LittleEndian.PutUint32(out[:4], plen)
	copy(out[4:], payload)
	return out
}
