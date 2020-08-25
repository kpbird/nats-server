// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type mqttErrorReader struct {
	err error
}

func (r *mqttErrorReader) Read(b []byte) (int, error)      { return 0, r.err }
func (r *mqttErrorReader) SetReadDeadline(time.Time) error { return nil }

func testNewEOFReader() *mqttErrorReader {
	return &mqttErrorReader{err: io.EOF}
}

func TestMQTTReader(t *testing.T) {
	r := &mqttReader{}
	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err := r.readBytes("", false)
	if err != nil {
		t.Fatal(err)
	}
	sbs := string(bs)
	if sbs != "ab" {
		t.Fatalf(`expected "ab", got %q`, sbs)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err = r.readBytes("", true)
	if err != nil {
		t.Fatal(err)
	}
	bs[0], bs[1] = 'c', 'd'
	if bytes.Equal(bs, r.buf[2:]) {
		t.Fatal("readBytes should have returned a copy")
	}

	r.reset([]byte{'a', 'b'})
	if b, err := r.readByte(""); err != nil || b != 'a' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if !r.hasMore() {
		t.Fatal("expected to have more, did not")
	}
	if b, err := r.readByte(""); err != nil || b != 'b' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if r.hasMore() {
		t.Fatal("expected to not have more")
	}
	if _, err := r.readByte("test"); err == nil || !strings.Contains(err.Error(), "error reading test") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	if s, err := r.readString(""); err != nil || s != "ab" {
		t.Fatalf("Error reading string: s=%q err=%v", s, err)
	}

	r.reset([]byte{10})
	if _, err := r.readUint16("uint16"); err == nil || !strings.Contains(err.Error(), "error reading uint16") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{1, 2, 3})
	r.reader = testNewEOFReader()
	if err := r.ensurePacketInBuffer(10); err == nil || !strings.Contains(err.Error(), "error ensuring protocol is loaded") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0x82, 0xff, 0x3})
	l, err := r.readPacketLen()
	if err != nil {
		t.Fatal("error getting packet len")
	}
	if l != 0xff82 {
		t.Fatalf("expected length 0xff82 got 0x%x", l)
	}
	r.reset([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	if _, err := r.readPacketLen(); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("unexpected error: %v", err)
	}
	r.reset([]byte{0x80})
	if _, err := r.readPacketLen(); err != io.ErrUnexpectedEOF {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0x80})
	r.reader = &mqttErrorReader{err: errors.New("on purpose")}
	if _, err := r.readPacketLen(); err == nil || !strings.Contains(err.Error(), "on purpose") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMQTTWriter(t *testing.T) {
	w := &mqttWriter{}
	w.WriteUint16(1234)

	r := &mqttReader{}
	r.reset(w.Bytes())
	if v, err := r.readUint16(""); err != nil || v != 1234 {
		t.Fatalf("unexpected value: v=%v err=%v", v, err)
	}

	w.Reset()
	w.WriteString("test")
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before string, got %v", r.buf)
	}

	w.Reset()
	w.WriteBytes([]byte("test"))
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before bytes, got %v", r.buf)
	}

	ints := []int{
		0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455,
	}
	lens := []int{
		1, 1, 1, 2, 2, 3, 3, 4, 4,
	}

	tl := 0
	w.Reset()
	for i, v := range ints {
		w.WriteVarInt(v)
		tl += lens[i]
		if tl != w.Len() {
			t.Fatalf("expected len %d, got %d", tl, w.Len())
		}
	}

	r.reset(w.Bytes())
	for _, v := range ints {
		x, _ := r.readPacketLen()
		if v != x {
			t.Fatalf("expected %d, got %d", v, x)
		}
	}
}

func testMQTTDefaultOptions() *Options {
	o := DefaultOptions()
	o.Cluster.Port = 0
	o.Gateway.Name = ""
	o.Gateway.Port = 0
	o.LeafNode.Port = 0
	o.Websocket.Port = 0
	o.MQTT.Host = "127.0.0.1"
	o.MQTT.Port = -1
	return o
}

func testMQTTRunServer(t testing.TB, o *Options) *Server {
	o.NoLog = false
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	l := &DummyLogger{}
	s.SetLogger(l, true, true)
	go s.Start()
	if !s.ReadyForConnections(3 * time.Second) {
		t.Fatal("Unable to start server")
	}
	return s
}

func testMQTTDefaultTLSOptions(t *testing.T, verify bool) *Options {
	t.Helper()
	o := testMQTTDefaultOptions()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
		Verify:   verify,
	}
	var err error
	o.MQTT.TLSConfig, err = GenTLSConfig(tc)
	o.MQTT.TLSTimeout = 2.0
	if err != nil {
		t.Fatalf("Error creating tls config: %v", err)
	}
	return o
}

func TestMQTTConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()
	if o.MQTT.TLSConfig == nil {
		t.Fatal("expected TLS config to be set")
	}
}

func TestMQTTStart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	nc.Close()

	// Check failure to start due to port in use
	o2 := testMQTTDefaultOptions()
	o2.MQTT.Port = o.MQTT.Port
	s2, err := NewServer(o2)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s2.Shutdown()
	l := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s2.SetLogger(l, false, false)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s2.Start()
		wg.Done()
	}()

	select {
	case e := <-l.fatalCh:
		if !strings.Contains(e, "Unable to listen for MQTT connections") {
			t.Fatalf("Unexpected error: %q", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Should have gotten a fatal error")
	}
}

func TestMQTTTLS(t *testing.T) {
	o := testMQTTDefaultTLSOptions(t, false)
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()
	// Set MaxVersion to TLSv1.2 so that we fail on handshake if there is
	// a disagreement between server and client.
	tlsc := &tls.Config{
		MaxVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Error doing tls handshake: %v", err)
	}
	nc.Close()
	s.Shutdown()

	// Force client cert verification
	o = testMQTTDefaultTLSOptions(t, true)
	s = testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc, err = net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()
	// Set MaxVersion to TLSv1.2 so that we fail on handshake if there is
	// a disagreement between server and client.
	tlsc = &tls.Config{
		MaxVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err == nil {
		t.Fatal("Handshake expected to fail since client did not provide cert")
	}
	nc.Close()

	// Add client cert.
	nc, err = net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()

	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/client-cert.pem",
		KeyFile:  "../test/configs/certs/client-key.pem",
	}
	tlsc, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	tlsc.InsecureSkipVerify = true
	tlsConn = tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Handshake error: %v", err)
	}
	nc.Close()
	s.Shutdown()

	// Lower TLS timeout so low that we should fail
	o.MQTT.TLSTimeout = 0.001
	s = testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc, err = net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()
	time.Sleep(100 * time.Millisecond)
	tlsConn = tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

type mqttConnInfo struct {
	clientID  string
	cleanSess bool
	keepAlive uint16
	will      *mqttWill
	user      string
	pass      string
}

func testMQTTRead(c net.Conn) ([]byte, error) {
	var buf [512]byte
	// Make sure that test does not block
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(buf[:])
	if err != nil {
		return nil, err
	}
	c.SetReadDeadline(time.Time{})
	return copyBytes(buf[:n]), nil
}

func testMQTTWrite(c net.Conn, buf []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Write(buf)
	c.SetWriteDeadline(time.Time{})
	return n, err
}

func testMQTTConnect(t testing.TB, ci *mqttConnInfo, host string, port int) (net.Conn, *mqttReader) {
	t.Helper()

	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}

	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}

	buf, err := testMQTTRead(c)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}
	br := &mqttReader{reader: c}
	br.reset(buf)

	return c, br
}

func mqttCreateConnectProto(ci *mqttConnInfo) []byte {
	flags := byte(0)
	if ci.cleanSess {
		flags |= mqttConnFlagCleanSession
	}
	if ci.will != nil {
		flags |= mqttConnFlagWillFlag | (ci.will.qos << 3)
		if ci.will.retain {
			flags |= mqttConnFlagWillRetain
		}
	}
	if ci.user != _EMPTY_ {
		flags |= mqttConnFlagUsernameFlag
	}
	if ci.pass != _EMPTY_ {
		flags |= mqttConnFlagPasswordFlag
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len(ci.clientID)

	if ci.will != nil {
		pkLen += 2 + len(ci.will.topic)
		pkLen += 2 + len(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		pkLen += 2 + len(ci.user)
	}
	if ci.pass != _EMPTY_ {
		pkLen += 2 + len(ci.pass)
	}

	w := &mqttWriter{}
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x4)
	w.WriteByte(flags)
	w.WriteUint16(ci.keepAlive)
	w.WriteString(ci.clientID)
	if ci.will != nil {
		w.WriteBytes(ci.will.topic)
		w.WriteBytes(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		w.WriteString(ci.user)
	}
	if ci.pass != _EMPTY_ {
		w.WriteBytes([]byte(ci.pass))
	}
	return w.Bytes()
}

func testMQTTCheckConnAck(t testing.TB, r *mqttReader, rc byte, sessionPresent bool) {
	t.Helper()
	r.reader.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := r.ensurePacketInBuffer(4); err != nil {
		t.Fatalf("Error ensuring packet in buffer: %v", err)
	}
	r.reader.SetReadDeadline(time.Time{})
	b, err := r.readByte("connack packet type")
	if err != nil {
		t.Fatalf("Error reading packet type: %v", err)
	}
	pt := b & mqttPacketMask
	if pt != mqttPacketConnectAck {
		t.Fatalf("Expected ConnAck (%x), got %x", mqttPacketConnectAck, pt)
	}
	pl, err := r.readByte("connack packet len")
	if err != nil {
		t.Fatalf("Error reading packet length: %v", err)
	}
	if pl != 2 {
		t.Fatalf("ConnAck packet length should be 2, got %v", pl)
	}
	caf, err := r.readByte("connack flags")
	if err != nil {
		t.Fatalf("Error reading packet length: %v", err)
	}
	if caf&0xfe != 0 {
		t.Fatalf("ConnAck flag bits 7-1 should all be 0, got %x", caf>>1)
	}
	if sp := caf == 1; sp != sessionPresent {
		t.Fatalf("Expected session present flag=%v got %v", sessionPresent, sp)
	}
	carc, err := r.readByte("connack return code")
	if err != nil {
		t.Fatalf("Error reading returned code: %v", err)
	}
	if carc != rc {
		t.Fatalf("Expected return code to be %v, got %v", rc, carc)
	}
}

func TestMQTTTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{&User{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		mqttUsers   bool
		provideCert bool
	}{
		{"no users override, client provides cert", false, true},
		{"no users override, client does not provide cert", false, false},
		{"users override, client provides cert", true, true},
		{"users override, client does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.Host = "localhost"
			o.Accounts = []*Account{acc}
			if test.mqttUsers {
				o.MQTT.Users = users
			} else {
				o.Users = users
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.MQTT.TLSConfig = tlsc
			o.MQTT.TLSTimeout = 2.0
			o.MQTT.TLSMap = true
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()

			addr := fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port)
			mc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Error creating ws connection: %v", err)
			}
			defer mc.Close()
			tlscc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				tlscc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			tlscc.InsecureSkipVerify = true
			if test.provideCert {
				tlscc.MinVersion = tls.VersionTLS13
			}
			mc = tls.Client(mc, tlscc)
			if err := mc.(*tls.Conn).Handshake(); err != nil {
				t.Fatalf("Error during handshake: %v", err)
			}

			ci := &mqttConnInfo{cleanSess: true}
			proto := mqttCreateConnectProto(ci)
			if _, err := testMQTTWrite(mc, proto); err != nil {
				t.Fatalf("Error sending proto: %v", err)
			}
			buf, err := testMQTTRead(mc)
			if !test.provideCert {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				} else if !strings.Contains(err.Error(), "bad certificate") {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error reading: %v", err)
			}
			r := &mqttReader{reader: mc}
			r.reset(buf)
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			var c *client
			s.mu.Lock()
			for _, sc := range s.clients {
				sc.mu.Lock()
				if sc.mqtt != nil {
					c = sc
				}
				sc.mu.Unlock()
				if c != nil {
					break
				}
			}
			s.mu.Unlock()
			if c == nil {
				t.Fatal("Client not found")
			}

			var uname string
			var accname string
			c.mu.Lock()
			uname = c.opts.Username
			if c.acc != nil {
				accname = c.acc.GetName()
			}
			c.mu.Unlock()
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}

func TestMQTTBasicAuth(t *testing.T) {
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		rc   byte
	}{
		{
			"top level auth, no override, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      test.user,
				pass:      test.pass,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTAuthTimeout(t *testing.T) {
	for _, test := range []struct {
		name string
		at   float64
		mat  float64
		ok   bool
	}{
		{"use top-level auth timeout", 10.0, 0.0, true},
		{"use mqtt auth timeout", 10.0, 0.05, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.AuthTimeout = test.at
			o.MQTT.Username = "mqtt"
			o.MQTT.Password = "client"
			o.MQTT.AuthTimeout = test.mat
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()

			mc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
			if err != nil {
				t.Fatalf("Error connecting: %v", err)
			}
			defer mc.Close()

			time.Sleep(100 * time.Millisecond)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "mqtt",
				pass:      "client",
			}
			proto := mqttCreateConnectProto(ci)
			if _, err := testMQTTWrite(mc, proto); err != nil {
				if test.ok {
					t.Fatalf("Error sending connect: %v", err)
				}
				// else it is ok since we got disconnected due to auth timeout
				return
			}
			buf, err := testMQTTRead(mc)
			if err != nil {
				if test.ok {
					t.Fatalf("Error reading: %v", err)
				}
				// else it is ok since we got disconnected due to auth timeout
				return
			}
			r := &mqttReader{reader: mc}
			r.reset(buf)
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		})
	}
}

func TestMQTTTokenAuth(t *testing.T) {
	for _, test := range []struct {
		name  string
		opts  func() *Options
		token string
		rc    byte
	}{
		{
			"top level auth, no override, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"badtoken", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"goodtoken", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Token = "goodtoken"
				return o
			},
			"badtoken", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Token = "goodtoken"
				return o
			},
			"goodtoken", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "clienttoken"
				o.MQTT.Token = "mqtttoken"
				return o
			},
			"clienttoken", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "clienttoken"
				o.MQTT.Token = "mqtttoken"
				return o
			},
			"mqtttoken", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "ignore_use_token",
				pass:      test.token,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTUsersAuth(t *testing.T) {
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		rc   byte
	}{
		{
			"top level auth, no override, wrong user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = []*User{
					&User{Username: "normal1", Password: "client1"},
					&User{Username: "normal2", Password: "client2"},
				}
				return o
			},
			"mqtt", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = []*User{
					&User{Username: "normal1", Password: "client1"},
					&User{Username: "normal2", Password: "client2"},
				}
				return o
			},
			"normal2", "client2", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Users = []*User{
					&User{Username: "mqtt1", Password: "client1"},
					&User{Username: "mqtt2", Password: "client2"},
				}
				return o
			},
			"mqtt", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Users = []*User{
					&User{Username: "mqtt1", Password: "client1"},
					&User{Username: "mqtt2", Password: "client2"},
				}
				return o
			},
			"mqtt1", "client1", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = []*User{
					&User{Username: "normal1", Password: "client1"},
					&User{Username: "normal2", Password: "client2"},
				}
				o.MQTT.Users = []*User{
					&User{Username: "mqtt1", Password: "client1"},
					&User{Username: "mqtt2", Password: "client2"},
				}
				return o
			},
			"normal2", "client2", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = []*User{
					&User{Username: "normal1", Password: "client1"},
					&User{Username: "normal2", Password: "client2"},
				}
				o.MQTT.Users = []*User{
					&User{Username: "mqtt1", Password: "client1"},
					&User{Username: "mqtt2", Password: "client2"},
				}
				return o
			},
			"mqtt2", "client2", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      test.user,
				pass:      test.pass,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTNoAuthUserValidation(t *testing.T) {
	// It is illegal to configure a mqtt's NoAuthUser if mqtt's
	// auth config does not have a matching User.
	// Create regular clients's User to make sure that we fail even if
	// the mqtt's NoAuthUser is found in opts.Users.
	o := testMQTTDefaultOptions()
	o.Users = []*User{&User{Username: "user", Password: "pwd"}}
	o.MQTT.NoAuthUser = "user"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "users are not") {
		t.Fatalf("Expected error saying that users are not configured, got %v", err)
	}

	o.MQTT.Users = []*User{&User{Username: "msuser", Password: "pwd"}}
	o.MQTT.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected error saying no auth user not found, got %v", err)
	}
}

func TestMQTTNoAuthUser(t *testing.T) {
	for _, test := range []struct {
		name            string
		createMQTTUsers bool
		useAuth         bool
		expectedUser    string
		expectedAcc     string
	}{
		{"no override, no user provided", false, false, "noauth", "normal"},
		{"no override, user povided", false, true, "user", "normal"},
		{"override, no user provided", true, false, "mqttnoauth", "mqtt"},
		{"override, user provided", true, true, "mqttuser", "mqtt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			normalAcc := NewAccount("normal")
			mqttAcc := NewAccount("mqtt")
			o.Accounts = []*Account{normalAcc, mqttAcc}
			o.Users = []*User{
				&User{Username: "noauth", Password: "pwd", Account: normalAcc},
				&User{Username: "user", Password: "pwd", Account: normalAcc},
			}
			o.NoAuthUser = "noauth"
			if test.createMQTTUsers {
				o.MQTT.Users = []*User{
					&User{Username: "mqttnoauth", Password: "pwd", Account: mqttAcc},
					&User{Username: "mqttuser", Password: "pwd", Account: mqttAcc},
				}
				o.MQTT.NoAuthUser = "mqttnoauth"
			}
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()

			ci := &mqttConnInfo{cleanSess: true}
			if test.useAuth {
				ci.user = test.expectedUser
				ci.pass = "pwd"
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			var c *client
			s.mu.Lock()
			for _, sc := range s.clients {
				sc.mu.Lock()
				if sc.mqtt != nil {
					c = sc
				}
				sc.mu.Unlock()
				if c != nil {
					break
				}
			}
			s.mu.Unlock()
			if c == nil {
				t.Fatal("Client not found")
			}
			c.mu.Lock()
			uname := c.opts.Username
			aname := c.acc.GetName()
			c.mu.Unlock()
			if uname != test.expectedUser {
				t.Fatalf("Expected selected user to be %q, got %q", test.expectedUser, uname)
			}
			if aname != test.expectedAcc {
				t.Fatalf("Expected selected account to be %q, got %q", test.expectedAcc, aname)
			}
		})
	}
}

func TestMQTTConnectNotFirstProto(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	w := &mqttWriter{}
	mqttWritePublish(w, 0, false, false, "foo", 0, []byte("hello"))
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	testMQTTExpectDisconnect(t, c)
}

func TestMQTTSecondConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true})
	if _, err := testMQTTWrite(mc, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTParseConnect(t *testing.T) {
	eofr := testNewEOFReader()
	for _, test := range []struct {
		name   string
		proto  []byte
		pl     int
		reader mqttIOReader
		err    string
	}{
		{"packet in buffer error", nil, 10, eofr, "error ensuring protocol is loaded"},
		{"bad proto name", []byte{0, 4, 'B', 'A', 'D'}, 5, nil, "protocol name"},
		{"invalid proto name", []byte{0, 3, 'B', 'A', 'D'}, 5, nil, "expected connect packet with protocol name"},
		{"old proto not supported", []byte{0, 6, 'M', 'Q', 'I', 's', 'd', 'p'}, 8, nil, "older protocol"},
		{"error on protocol level", []byte{0, 4, 'M', 'Q', 'T', 'T'}, 6, eofr, "protocol level"},
		{"unacceptable protocol version", []byte{0, 4, 'M', 'Q', 'T', 'T', 10}, 7, nil, "unacceptable protocol version"},
		{"error on flags", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel}, 7, eofr, "flags"},
		{"reserved flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1}, 8, nil, "connect flags reserved bit not set to 0"},
		{"will qos without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 3}, 8, nil, "if Will flag is set to 0, Will QoS must be 0 too"},
		{"will retain without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 5}, 8, nil, "if Will flag is set to 0, Will Retain flag must be 0 too"},
		{"will qos", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 3<<3 | 1<<2}, 8, nil, "if Will flag is set to 1, Will QoS can be 0, 1 or 2"},
		{"no user but password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagPasswordFlag}, 8, nil, "password flag set but username flag is not"},
		{"missing keep alive", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0}, 8, nil, "keep alive"},
		{"missing client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1}, 10, nil, "client ID"},
		{"empty client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 0}, 12, nil, "when client ID is empty, clean session flag must be set to 1"},
		{"invalid utf8 client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 1, 241}, 13, nil, "invalid utf8 for client ID"},
		{"missing will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, 12, nil, "Will topic"},
		{"empty will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, 14, nil, "empty Will topic not allowed"},
		{"invalid utf8 will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, 15, nil, "invalide utf8 for Will topic"},
		{"invalid wildcard will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, '#'}, 15, nil, "wildcards not allowed"},
		{"error on will message", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a', 0, 3}, 17, eofr, "Will message"},
		{"error on username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, 12, eofr, "user name"},
		{"empty username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, 14, nil, "empty user name not allowed"},
		{"invalid utf8 username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, 15, nil, "invalid utf8 for user name"},
		{"error on password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagPasswordFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a'}, 15, eofr, "password"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{reader: test.reader}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			if _, _, err := c.mqttParseConnect(r, test.pl); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTConnKeepAlive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, keepAlive: 1}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))

	time.Sleep(2 * time.Second)
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTTopicConversion(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub := natsSubSync(t, nc, ">")
	natsFlush(t, nc)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
	}{
		{"single level", "foo", "foo"},
		{"dot is slash", "foo.bar", "foo/bar"},
		{"slash is dot", "foo/bar", "foo.bar"},
		{"single slash remains slash", "/", "/"},
		{"slash first is slash dot", "/foo", "/.foo"},
		{"slash first is slash dot with several sep ", "/foo/bar", "/.foo.bar"},
		{"slash last is dot slash", "foo/", "foo./"},
		{"slash last is dot slash with several sep", "foo/bar/", "foo.bar./"},
		{"slash is first and last", "/foo/bar/baz/", "/.foo.bar.baz./"},
		{"topic has dot and slash last", "foo./", "foo/./"},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := &mqttWriter{}
			mqttWritePublish(w, 0, false, false, test.mqttTopic, 0, []byte("hello"))
			if _, err := testMQTTWrite(mc, w.Bytes()); err != nil {
				t.Fatalf("Error publishing: %v", err)
			}
			msg := natsNexMsg(t, sub, time.Second)
			if msg.Subject != test.natsSubject {
				t.Fatalf("MQTT published on %q, expected NATS subject to be %q, got %q",
					test.mqttTopic, test.natsSubject, msg.Subject)
			}
		})
	}
}

func TestMQTTSubjectConversion(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	for _, test := range []struct {
		name        string
		natsSubject string
		mqttTopic   string
	}{
		{"single level", "foo", "foo"},
		{"dot is slash", "foo.bar", "foo/bar"},
		{"slash is dot", "foo/bar", "foo.bar"},
		{"single slash remains slash", "/", "/"},
		{"first slash dot becomes slash", "/.foo", "/foo"},
		{"first slash dot becomes slash several levels", "/.foo.bar", "/foo/bar"},
		{"ends with dot and slash", "foo./", "foo/"},
		{"ends with dot and slash several levels", "foo.bar./", "foo/bar/"},
		{"slash and dot first and last", "/.foo.bar.baz./", "/foo/bar/baz/"},
		{"ends with slash dot slash", "foo/./", "foo./"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("#"), qos: 0}}, []byte{0})
			testMQTTFlush(t, mc, nil, r)

			natsPub(t, nc, test.natsSubject, []byte("hello"))
			testMQTTCheckPubMsg(t, mc, r, test.mqttTopic, 0, []byte("hello"))
		})
	}
}

func testMQTTReaderHasAtLeastOne(t testing.TB, r *mqttReader) {
	t.Helper()
	r.reader.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := r.ensurePacketInBuffer(1); err != nil {
		t.Fatal(err)
	}
	r.reader.SetReadDeadline(time.Time{})
}

func TestMQTTParseSub(t *testing.T) {
	eofr := testNewEOFReader()
	for _, test := range []struct {
		name   string
		proto  []byte
		b      byte
		pl     int
		reader mqttIOReader
		err    string
	}{
		{"reserved flag", nil, 3, 0, nil, "wrong subscribe reserved flags"},
		{"ensure packet loaded", []byte{1, 2}, mqttSubscribeFlags, 10, eofr, "error ensuring protocol is loaded"},
		{"error reading packet id", []byte{1}, mqttSubscribeFlags, 1, eofr, "reading packet identifier"},
		{"missing filters", []byte{0, 1}, mqttSubscribeFlags, 2, nil, "subscribe protocol must contain at least 1 topic filter"},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttSubscribeFlags, 5, eofr, "topic filter"},
		{"empty topic", []byte{0, 1, 0, 0}, mqttSubscribeFlags, 4, nil, "topic filter cannot be empty"},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttSubscribeFlags, 5, nil, "invalid utf8 for topic filter"},
		{"missing qos", []byte{0, 1, 0, 1, 'a'}, mqttSubscribeFlags, 5, nil, "QoS"},
		{"invalid qos", []byte{0, 1, 0, 1, 'a', 3}, mqttSubscribeFlags, 6, nil, "subscribe QoS value must be 0, 1 or 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{reader: test.reader}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			if _, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, true); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTSub(t testing.TB, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter, expected []byte) {
	t.Helper()
	w := &mqttWriter{}
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter) + 1
	}
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes(f.filter)
		w.WriteByte(f.qos)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing SUBSCRIBE protocol: %v", err)
	}
	// Make sure we have at least 1 byte in buffer (if not will read)
	testMQTTReaderHasAtLeastOne(t, r)
	// Parse SUBACK
	b, err := r.readByte("packet type")
	if err != nil {
		t.Fatal(err)
	}
	if pt := b & mqttPacketMask; pt != mqttPacketSubAck {
		t.Fatalf("Expected SUBACK packet %x, got %x", mqttPacketSubAck, pt)
	}
	pl, err := r.readPacketLen()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePacketInBuffer(pl); err != nil {
		t.Fatal(err)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
	for i, rem := 0, pl-2; rem > 0; rem-- {
		qos, err := r.readByte("filter qos")
		if err != nil {
			t.Fatal(err)
		}
		if qos != expected[i] {
			t.Fatalf("For topic filter %q expected qos of %v, got %v",
				filters[i].filter, expected[i], qos)
		}
		i++
	}
}

func TestMQTTSubAck(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	subs := []*mqttFilter{
		{filter: []byte("foo"), qos: 0},
		{filter: []byte("bar"), qos: 1},
		{filter: []byte("baz"), qos: 2},       // Since we don't support, we should receive a result of 1
		{filter: []byte("foo/#/bar"), qos: 0}, // Invalid sub, so we should receive a result of mqttSubAckFailure
	}
	expected := []byte{
		0,
		1,
		1,
		mqttSubAckFailure,
	}
	testMQTTSub(t, 1, mc, r, subs, expected)
}

func testMQTTFlush(t testing.TB, c net.Conn, bw *bufio.Writer, r *mqttReader) {
	t.Helper()
	w := &mqttWriter{}
	w.WriteByte(mqttPacketPing)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	r.ensurePacketInBuffer(2)
	ab, err := r.readByte("pingresp")
	if err != nil {
		t.Fatalf("Error reading ping response: %v", err)
	}
	if pt := ab & mqttPacketMask; pt != mqttPacketPingResp {
		t.Fatalf("Expected ping response got %x", pt)
	}
	l, err := r.readPacketLen()
	if err != nil {
		t.Fatal(err)
	}
	if l != 0 {
		t.Fatalf("Expected PINGRESP length to be 0, got %v", l)
	}
}

func testMQTTExpectNothing(t testing.TB, r *mqttReader) {
	t.Helper()
	r.reader.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if err := r.ensurePacketInBuffer(1); err == nil {
		t.Fatalf("Expected nothing, got %v", r.buf[r.pos:])
	}
	r.reader.SetReadDeadline(time.Time{})
}

func testMQTTCheckPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, flags byte, payload []byte) {
	t.Helper()
	pflags, pi := testMQTTGetPubMsg(t, c, r, topic, payload)
	if pflags != flags {
		t.Fatalf("Expected flags to be %x, got %x", flags, pflags)
	}
	if pi > 0 {
		testMQTTSendPubAck(t, c, pi)
	}
}

func testMQTTGetPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16) {
	t.Helper()
	testMQTTReaderHasAtLeastOne(t, r)
	b, err := r.readByte("packet type")
	if err != nil {
		t.Fatal(err)
	}
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
	}
	pflags := b & mqttPacketFlagMask
	qos := (pflags & mqttPubFlagQoS) >> 1
	pl, err := r.readPacketLen()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePacketInBuffer(pl); err != nil {
		t.Fatal(err)
	}
	start := r.pos
	ptopic, err := r.readString("topic name")
	if err != nil {
		t.Fatal(err)
	}
	if ptopic != topic {
		t.Fatalf("Expected topic %q, got %q", topic, ptopic)
	}
	var pi uint16
	if qos > 0 {
		pi, err = r.readUint16("packet identifier")
		if err != nil {
			t.Fatal(err)
		}
	}
	msgLen := pl - (r.pos - start)
	if r.pos+msgLen > len(r.buf) {
		t.Fatalf("computed message length goes beyond buffer: ml=%v pos=%v lenBuf=%v",
			msgLen, r.pos, len(r.buf))
	}
	ppayload := r.buf[r.pos : r.pos+msgLen]
	if !bytes.Equal(payload, ppayload) {
		t.Fatalf("Expected payload %q, got %q", payload, ppayload)
	}
	r.pos += msgLen
	return pflags, pi
}

func testMQTTSendPubAck(t testing.TB, c net.Conn, pi uint16) {
	t.Helper()
	w := &mqttWriter{}
	w.WriteByte(mqttPacketPubAck)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing PUBACK: %v", err)
	}
}

func testMQTTPublish(t testing.TB, c net.Conn, r *mqttReader, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	w := &mqttWriter{}
	mqttWritePublish(w, qos, dup, retain, topic, pi, payload)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing PUBLISH proto: %v", err)
	}
	if qos > 0 {
		// Since we don't support QoS 2, we should get disconnected
		if qos == 2 {
			testMQTTExpectDisconnect(t, c)
			return
		}
		testMQTTReaderHasAtLeastOne(t, r)
		// Parse PUBACK
		b, err := r.readByte("packet type")
		if err != nil {
			t.Fatal(err)
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPubAck {
			t.Fatalf("Expected PUBACK packet %x, got %x", mqttPacketPubAck, pt)
		}
		pl, err := r.readPacketLen()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.ensurePacketInBuffer(pl); err != nil {
			t.Fatal(err)
		}
		rpi, err := r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}
	}
}

func TestMQTTParsePub(t *testing.T) {
	eofr := testNewEOFReader()
	for _, test := range []struct {
		name   string
		flags  byte
		proto  []byte
		pl     int
		reader mqttIOReader
		err    string
	}{
		{"qos not supported", 0x4, nil, 0, nil, "not supported"},
		{"packet in buffer error", 0, nil, 10, eofr, "error ensuring protocol is loaded"},
		{"error on topic", 0, []byte{0, 3, 'f', 'o'}, 4, eofr, "topic"},
		{"empty topic", 0, []byte{0, 0}, 2, nil, "topic cannot be empty"},
		{"wildcards topic", 0, []byte{0, 1, '#'}, 3, nil, "wildcards not allowed"},
		{"error on packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o'}, 5, eofr, "packet identifier"},
		{"invalid packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o', 0, 0}, 7, nil, "packet identifier cannot be 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{reader: test.reader}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			pp := &mqttPublish{flags: test.flags}
			if err := c.mqttParsePub(r, test.pl, pp); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTParsePubAck(t *testing.T) {
	eofr := testNewEOFReader()
	for _, test := range []struct {
		name   string
		proto  []byte
		pl     int
		reader mqttIOReader
		err    string
	}{
		{"packet in buffer error", nil, 10, eofr, "error ensuring protocol is loaded"},
		{"error reading packet identifier", []byte{0}, 1, eofr, "packet identifier"},
		{"invalid packet identifier", []byte{0, 0}, 2, nil, "packet identifier cannot be 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{reader: test.reader}
			r.reset(test.proto)
			if _, err := mqttParsePubAck(r, test.pl); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTPublish(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mcp, mpr, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 2, false, false, "foo", 2, []byte("msg"))
}

func TestMQTTSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	for _, test := range []struct {
		name           string
		mqttSubTopic   string
		natsPubSubject string
		mqttPubTopic   string
		ok             bool
	}{
		{"1 level match", "foo", "foo", "foo", true},
		{"1 level no match", "foo", "bar", "bar", false},
		{"2 levels match", "foo/bar", "foo.bar", "foo/bar", true},
		{"2 levels no match", "foo/bar", "foo.baz", "foo/baz", false},
		{"3 levels match", "/foo/bar", "/.foo.bar", "/foo/bar", true},
		{"3 levels no match", "/foo/bar", "/.foo.baz", "/foo/baz", false},

		{"single level wc", "foo/+", "foo.bar.baz", "foo/bar/baz", false},
		{"single level wc", "foo/+", "foo.bar./", "foo/bar/", false},
		{"single level wc", "foo/+", "foo.bar", "foo/bar", true},
		{"single level wc", "foo/+", "foo./", "foo/", true},
		{"single level wc", "foo/+", "foo", "foo", false},
		{"single level wc", "foo/+", "/.foo", "/foo", false},

		{"multiple level wc", "foo/#", "foo.bar.baz./", "foo/bar/baz/", true},
		{"multiple level wc", "foo/#", "foo.bar.baz", "foo/bar/baz", true},
		{"multiple level wc", "foo/#", "foo.bar./", "foo/bar/", true},
		{"multiple level wc", "foo/#", "foo.bar", "foo/bar", true},
		{"multiple level wc", "foo/#", "foo./", "foo/", true},
		{"multiple level wc", "foo/#", "foo", "foo", true},
		{"multiple level wc", "foo/#", "/.foo", "/foo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte(test.mqttSubTopic), qos: 0}}, []byte{0})
			testMQTTFlush(t, mc, nil, r)

			natsPub(t, nc, test.natsPubSubject, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}

			testMQTTPublish(t, mcp, mpr, 0, false, false, test.mqttPubTopic, 0, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}
		})
	}
}

func TestMQTTSubQoS(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mqttTopic := "foo/bar"

	// Subscribe with QoS 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo/#"), qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte(mqttTopic), qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish from NATS, which means QoS 0
	natsPub(t, nc, "foo.bar", []byte("NATS"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))

	// Publish from MQTT with QoS 0
	testMQTTPublish(t, mcp, mpr, 0, false, false, mqttTopic, 0, []byte("msg"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))

	// Publish from MQTT with QoS 1
	testMQTTPublish(t, mcp, mpr, 1, false, false, mqttTopic, 1, []byte("msg"))
	pflags1, pi1 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags1 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags1)
	}
	pflags2, pi2 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags2 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags2)
	}
	if pi1 == pi2 {
		t.Fatalf("packet identifier for message 1: %v should be different from message 2", pi1)
	}
	testMQTTSendPubAck(t, mc, pi1)
	testMQTTSendPubAck(t, mc, pi2)
}

func getSubQoS(sub *subscription) int {
	if sub.mqtt != nil {
		return int(sub.mqtt.qos)
	}
	return -1
}

func TestMQTTSubDups(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Test with single SUBSCRIBE protocol but multiple filters
	filters := []*mqttFilter{
		&mqttFilter{filter: []byte("foo"), qos: 1},
		&mqttFilter{filter: []byte("foo"), qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{1, 0})
	testMQTTFlush(t, mc, nil, r)

	// And also with separate SUBSCRIBE protocols
	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("bar"), qos: 0}}, []byte{0})
	// Ask for QoS 2 but server will downgrade to 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("bar"), qos: 2}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received only once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Check that the QoS for subscriptions have been updated to the latest received filter
	var err error
	var subc *client
	s.mu.Lock()
	for _, c := range s.clients {
		c.mu.Lock()
		if c.opts.Username == "sub" {
			subc = c
			if sub := c.subs["foo"]; sub == nil || getSubQoS(sub) != 0 {
				err = fmt.Errorf("subscription foo QoS should be 0, got %v", getSubQoS(sub))
			}
			if err == nil {
				if sub := c.subs["bar"]; sub == nil || getSubQoS(sub) != 1 {
					err = fmt.Errorf("subscription bar QoS should be 1, got %v", getSubQoS(sub))
				}
			}
		}
		c.mu.Unlock()
		if subc != nil {
			break
		}
	}
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Now subscribe on "foo/#" which means that a PUBLISH on "foo" will be received
	// by this subscription and also the one on "foo".
	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo/#"), qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	checkWCSub := func(expectedQoS int) {
		t.Helper()

		subc.mu.Lock()
		defer subc.mu.Unlock()

		// We should have a subscription with key "foo.>" and "foo fwc"
		// along with existing "foo" and "bar"
		if lenmap := len(subc.subs); lenmap != 4 {
			t.Fatalf("Subs map should have 4 entries, got %v", lenmap)
		}
		if sub, ok := subc.subs["foo.>"]; !ok {
			t.Fatal("Expected sub foo.> to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo.> QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		if sub, ok := subc.subs["foo fwc"]; !ok {
			t.Fatal("Expected sub foo fwc to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo fwc QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		// Make sure existing sub on "foo" qos was not changed.
		if sub, ok := subc.subs["foo"]; !ok {
			t.Fatal("Expected sub foo to be present but was not")
		} else if getSubQoS(sub) != 0 {
			t.Fatalf("Expected sub foo QoS to be 0, got %v", getSubQoS(sub))
		}
	}
	checkWCSub(1)

	// Sub again on same subject with lower QoS
	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo/#"), qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	checkWCSub(0)
}

func TestMQTTSubWithSpaces(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo bar"), qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "foo bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo bar", 0, []byte("msg"))

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo_bar", []byte("nats"))
	testMQTTCheckPubMsg(t, mc, r, "foo bar", 0, []byte("nats"))
}

func TestMQTTSubCaseSensitive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("Foo/Bar"), qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "Foo/Bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("msg"))

	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "Foo.Bar", []byte("nats"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("nats"))

	natsPub(t, nc, "foo.bar", []byte("nats"))
	testMQTTExpectNothing(t, r)
}

func TestMQTTSubPropagation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Cluster.Host = "127.0.0.1"
	o.Cluster.Port = -1
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	o2 := DefaultOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s, s2)

	nc := natsConnect(t, s2.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo/#"), qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Because in MQTT foo/# means foo.> but also foo, check that this is propagated
	checkSubInterest(t, s2, globalAccountName, "foo", time.Second)

	// Publish on foo.bar, foo./ and foo and we should receive them
	natsPub(t, nc, "foo.bar", []byte("hello"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("hello"))

	natsPub(t, nc, "foo./", []byte("from"))
	testMQTTCheckPubMsg(t, mc, r, "foo/", 0, []byte("from"))

	natsPub(t, nc, "foo", []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("NATS"))
}

func TestMQTTParseUnsub(t *testing.T) {
	eofr := testNewEOFReader()
	for _, test := range []struct {
		name   string
		proto  []byte
		b      byte
		pl     int
		reader mqttIOReader
		err    string
	}{
		{"reserved flag", nil, 3, 0, nil, "wrong unsubscribe reserved flags"},
		{"ensure packet loaded", []byte{1, 2}, mqttUnsubscribeFlags, 10, eofr, "error ensuring protocol is loaded"},
		{"error reading packet id", []byte{1}, mqttUnsubscribeFlags, 1, eofr, "reading packet identifier"},
		{"missing filters", []byte{0, 1}, mqttUnsubscribeFlags, 2, nil, "subscribe protocol must contain at least 1 topic filter"},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttUnsubscribeFlags, 5, eofr, "topic filter"},
		{"empty topic", []byte{0, 1, 0, 0}, mqttUnsubscribeFlags, 4, nil, "topic filter cannot be empty"},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttUnsubscribeFlags, 5, nil, "invalid utf8 for topic filter"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{reader: test.reader}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			if _, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, false); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTUnsub(t *testing.T, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter) {
	t.Helper()
	w := &mqttWriter{}
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter)
	}
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes(f.filter)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing UNSUBSCRIBE protocol: %v", err)
	}
	// Make sure we have at least 1 byte in buffer (if not will read)
	testMQTTReaderHasAtLeastOne(t, r)
	// Parse UNSUBACK
	b, err := r.readByte("packet type")
	if err != nil {
		t.Fatal(err)
	}
	if pt := b & mqttPacketMask; pt != mqttPacketUnsubAck {
		t.Fatalf("Expected UNSUBACK packet %x, got %x", mqttPacketUnsubAck, pt)
	}
	pl, err := r.readPacketLen()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePacketInBuffer(pl); err != nil {
		t.Fatal(err)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
}

func TestMQTTUnsub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo"), qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	// Unsubscribe
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo")}})

	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Use of wildcards subs
	filters := []*mqttFilter{
		&mqttFilter{filter: []byte("foo/bar"), qos: 0},
		&mqttFilter{filter: []byte("foo/#"), qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{0, 0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and check that message received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))

	// Unsub the wildcard one
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo/#")}})
	// Publish and check that message received once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Unsub last
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{&mqttFilter{filter: []byte("foo/bar")}})
	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)
}

func testMQTTExpectDisconnect(t testing.TB, c net.Conn) {
	if buf, err := testMQTTRead(c); err == nil {
		t.Fatalf("Expected connection to be disconnected, got %s", buf)
	}
}

func TestMQTTPublishTopicErrors(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	for _, test := range []struct {
		name  string
		topic string
	}{
		{"empty", ""},
		{"with single level wildcard", "foo/+"},
		{"with multiple level wildcard", "foo/#"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTPublish(t, mc, r, 0, false, false, test.topic, 0, []byte("msg"))
			testMQTTExpectDisconnect(t, mc)
		})
	}
}

func testMQTTDisconnect(t testing.TB, c net.Conn, bw *bufio.Writer) {
	t.Helper()
	w := &mqttWriter{}
	w.WriteByte(mqttPacketDisconnect)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	testMQTTExpectDisconnect(t, c)
}

func TestMQTTWill(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub := natsSubSync(t, nc, "will.topic")

	willMsg := []byte("bye")

	for _, test := range []struct {
		name         string
		willExpected bool
		willQoS      byte
	}{
		{"will qos 0", true, 0},
		{"will qos 1", true, 1},
		{"proper disconnect no will", false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{&mqttFilter{filter: []byte("will/#"), qos: 1}}, []byte{1})
			testMQTTFlush(t, mcs, nil, rs)

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   []byte("will/topic"),
						message: willMsg,
						qos:     test.willQoS,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			if test.willExpected {
				mc.Close()
				testMQTTCheckPubMsg(t, mcs, rs, "will/topic", test.willQoS<<1, willMsg)
				wm := natsNexMsg(t, sub, time.Second)
				if !bytes.Equal(wm.Data, willMsg) {
					t.Fatalf("Expected will message to be %q, got %q", willMsg, wm.Data)
				}
			} else {
				testMQTTDisconnect(t, mc, nil)
				testMQTTExpectNothing(t, rs)
				if wm, err := sub.NextMsg(100 * time.Millisecond); err == nil {
					t.Fatalf("Should not have receive a message, got %v", wm)
				}
			}
		})
	}
}

func TestMQTTWillRetain(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	willTopic := []byte("will/topic")
	willMsg := []byte("bye")

	mc, r := testMQTTConnect(t,
		&mqttConnInfo{
			cleanSess: true,
			will: &mqttWill{
				topic:   willTopic,
				message: willMsg,
				qos:     1,
				retain:  true,
			},
		}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, which will cause will to be produced with retain flag.
	mc.Close()

	// Create subscription on will topic and expect will message.
	mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{&mqttFilter{filter: []byte("will/#"), qos: 1}}, []byte{1})
	pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "will/topic", willMsg)
	if pflags&mqttPubFlagRetain == 0 {
		t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
	}
	if qos := pflags & mqttPubFlagQoS >> 1; qos != 1 {
		t.Fatalf("expected qos to be 1, got %v", qos)
	}
}

func TestMQTTWillRetainPermViolation(t *testing.T) {
	template := `
		port: -1
		authorization {
			mqtt_perms = {
				publish = ["%s"]
				subscribe = ["foo", "bar"]
			}
			users = [
				{user: mqtt, password: pass, permissions: $mqtt_perms}
			]
		}
		mqtt {
			port: -1
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "foo")))
	defer os.Remove(conf)

	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	ci := &mqttConnInfo{
		cleanSess: true,
		user:      "mqtt",
		pass:      "pass",
	}

	// We create first a connection with the Will topic that the publisher
	// is allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("foo"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, which will cause the Will to be sent with retain flag.
	mc.Close()

	// Create a subscription on the Will subject and we should receive it.
	ci.will = nil
	mcs, rs := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{&mqttFilter{filter: []byte("foo"), qos: 1}}, []byte{1})
	pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "foo", []byte("bye"))
	if pflags&mqttPubFlagRetain == 0 {
		t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
	}
	if qos := pflags & mqttPubFlagQoS >> 1; qos != 1 {
		t.Fatalf("expected qos to be 1, got %v", qos)
	}
	testMQTTDisconnect(t, mcs, nil)

	// Now create another connection with a Will that client is not allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("bar"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, to cause Will to be produced, but in that case should not be stored
	// since user not allowed to publish on "bar".
	mc.Close()

	// Create sub on "bar" which user is allowed to subscribe to.
	ci.will = nil
	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{&mqttFilter{filter: []byte("bar"), qos: 1}}, []byte{1})
	// No Will should be published since it should not have been stored in the first place.
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)

	// Now remove permission to publish on "foo" and check that a new subscription
	// on "foo" is now not getting the will message because the original user no
	// longer has permission to do so.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, "baz"))

	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{&mqttFilter{filter: []byte("foo"), qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)
}

func TestMQTTPublishRetain(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	for _, test := range []struct {
		name          string
		retained      bool
		sentValue     string
		expectedValue string
		subGetsIt     bool
	}{
		{"publish retained", true, "retained", "retained", true},
		{"publish not retained", false, "not retained", "retained", true},
		{"remove retained", true, "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, mc1, rs1, 0, false, test.retained, "foo", 0, []byte(test.sentValue))

			mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{&mqttFilter{filter: []byte("foo/#"), qos: 1}}, []byte{1})

			if test.subGetsIt {
				pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "foo", []byte(test.expectedValue))
				if pflags&mqttPubFlagRetain == 0 {
					t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
				}
			} else {
				testMQTTExpectNothing(t, rs2)
			}

			testMQTTDisconnect(t, mc1, nil)
			testMQTTDisconnect(t, mc2, nil)
		})
	}
}

func TestMQTTPublishRetainPermViolation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "mqtt",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo"}},
				Subscribe: &SubjectPermission{Allow: []string{"bar"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	ci := &mqttConnInfo{
		cleanSess: true,
		user:      "mqtt",
		pass:      "pass",
	}

	mc1, rs1 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc1, rs1, 0, false, true, "bar", 0, []byte("retained"))

	mc2, rs2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{&mqttFilter{filter: []byte("bar"), qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rs2)

	testMQTTDisconnect(t, mc1, nil)
	testMQTTDisconnect(t, mc2, nil)
}

// Benchmarks

func mqttBenchPub(b *testing.B, subject, payload string) {
	b.StopTimer()
	o := testMQTTDefaultOptions()
	s := RunServer(o)
	defer s.Shutdown()

	ci := &mqttConnInfo{cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)
	w := &mqttWriter{}
	mqttWritePublish(w, 0, false, false, subject, 0, []byte(payload))

	bw := bufio.NewWriterSize(c, 32768)
	sendOp := w.Bytes()
	b.SetBytes(int64(len(sendOp)))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	testMQTTFlush(b, c, bw, br)
	testMQTTDisconnect(b, c, bw)
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

var mqttPubSubj = "a"

func BenchmarkMQTT_QoS0_____Pub0b_Payload(b *testing.B) {
	mqttBenchPub(b, mqttPubSubj, "")
}

func BenchmarkMQTT_QoS0_____Pub8b_Payload(b *testing.B) {
	mqttBenchPub(b, mqttPubSubj, sizedString(8))
}

func BenchmarkMQTT_QoS0____Pub32b_Payload(b *testing.B) {
	mqttBenchPub(b, mqttPubSubj, sizedString(32))
}

func BenchmarkMQTT_QoS0___Pub128b_Payload(b *testing.B) {
	mqttBenchPub(b, mqttPubSubj, sizedString(128))
}

func BenchmarkMQTT_QoS0___Pub256b_Payload(b *testing.B) {
	mqttBenchPub(b, mqttPubSubj, sizedString(256))
}

func BenchmarkMQTT_QoS1_____Pub1K_Payload(b *testing.B) {
	mqttBenchPub(b, mqttPubSubj, sizedString(1024))
}
