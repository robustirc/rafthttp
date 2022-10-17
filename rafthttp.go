// Package rafthttp provides a HTTP/JSON-based raft transport.
package rafthttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/raft"
)

// Doer provides the Do() method, as found in net/http.Client.
//
// Using this interface instead of net/http.Client directly is useful so that
// users of the HTTPTransport can wrap requests to, for example, call
// req.SetBasicAuth.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// HTTPTransport provides a HTTP-based transport that can be used to
// communicate with Raft on remote machines. It is convenient to use if your
// application is an HTTP server already and you do not want to use multiple
// different transports (if not, you can use raft.NetworkTransport).
type HTTPTransport struct {
	logger   *log.Logger
	consumer chan raft.RPC
	addr     raft.ServerAddress
	client   Doer
	urlFmt   string
}

// NewHTTPTransport creates a new HTTP transport on the given addr.
//
// client must implement the Doer interface, but you can use e.g.
// net/http.DefaultClient if you do not need to wrap the Do() method.
//
// logger defaults to log.New(os.Stderr, "", log.LstdFlags) if nil.
//
// urlFmt defaults to "https://%v/raft/" and will be used in
// fmt.Sprintf(urlFmt+"/method", target) where method is the raft RPC method
// (e.g. appendEntries).
func NewHTTPTransport(addr raft.ServerAddress, client Doer, logger *log.Logger, urlFmt string) *HTTPTransport {
	if client == nil {
		client = http.DefaultClient
	}
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	if urlFmt == "" {
		urlFmt = "https://%v/raft/"
	}
	return &HTTPTransport{
		logger:   logger,
		consumer: make(chan raft.RPC),
		addr:     addr,
		client:   client,
		urlFmt:   urlFmt,
	}
}

type installSnapshotRequest struct {
	Args *raft.InstallSnapshotRequest
	Data []byte
}

func (t *HTTPTransport) send(url string, in, out interface{}) error {
	defer metrics.MeasureSince([]string{"raft", "httptransport", "latency"}, time.Now())
	buf, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("could not serialize request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	res, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not send request: %v", err)
	}

	defer func() {
		// Make sure to read the entire body and close the connection,
		// otherwise net/http cannot re-use the connection.
		ioutil.ReadAll(res.Body)
		res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status code: %v", res.Status)
	}

	return json.NewDecoder(res.Body).Decode(out)
}

// Consumer implements the raft.Transport interface.
func (t *HTTPTransport) Consumer() <-chan raft.RPC {
	return t.consumer
}

// LocalAddr implements the raft.Transport interface.
func (t *HTTPTransport) LocalAddr() raft.ServerAddress {
	return t.addr
}

// AppendEntriesPipeline implements the raft.Transport interface.
func (t *HTTPTransport) AppendEntriesPipeline(_ raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	// This transport does not support pipelining in the hashicorp/raft sense.
	// The underlying net/http reuses connections (keep-alive) and that is good
	// enough. We are talking about differences in the microsecond range, which
	// becomes irrelevant as soon as the raft nodes run on different computers.
	return nil, raft.ErrPipelineReplicationNotSupported
}

// AppendEntries implements the raft.Transport interface.
func (t *HTTPTransport) AppendEntries(_ raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return t.send(fmt.Sprintf(t.urlFmt+"AppendEntries", target), args, resp)
}

// RequestVote implements the raft.Transport interface.
func (t *HTTPTransport) RequestVote(_ raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.send(fmt.Sprintf(t.urlFmt+"RequestVote", target), args, resp)
}

// InstallSnapshot implements the raft.Transport interface.
func (t *HTTPTransport) InstallSnapshot(_ raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	defer metrics.MeasureSince([]string{"raft", "httptransport", "latency"}, time.Now())

	// Send a dummy request to see if the remote host supports
	// InstallSnapshotStreaming after all. We need to know whether we can use
	// InstallSnapshotStreaming or whether we need to fall back to
	// InstallSnapshot beforehand, because we cannot seek in |data|.
	url := fmt.Sprintf(t.urlFmt+"InstallSnapshotStreaming", target)
	probeReq, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}
	probeRes, err := t.client.Do(probeReq)
	if err != nil {
		return err
	}
	ioutil.ReadAll(probeRes.Body)
	probeRes.Body.Close()
	if probeRes.StatusCode == http.StatusNotFound {
		// Possibly the remote host runs an older version of the code
		// without the InstallSnapshotStreaming handler. Try the old
		// version.
		buf := make([]byte, 0, args.Size+bytes.MinRead)
		b := bytes.NewBuffer(buf)
		if _, err := io.CopyN(b, data, args.Size); err != nil {
			return fmt.Errorf("could not read data: %v", err)
		}
		buf = b.Bytes()
		return t.send(fmt.Sprintf(t.urlFmt+"InstallSnapshot", target), installSnapshotRequest{args, buf}, resp)
	}

	req, err := http.NewRequest("POST", url, data)
	if err != nil {
		return err
	}
	buf, err := json.Marshal(args)
	if err != nil {
		return err
	}
	req.Header.Set("X-InstallSnapshotRequest", string(buf))
	res, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not send request: %v", err)
	}

	defer func() {
		// Make sure to read the entire body and close the connection,
		// otherwise net/http cannot re-use the connection.
		ioutil.ReadAll(res.Body)
		res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("unexpected HTTP status code: %v (body: %s)", res.Status, strings.TrimSpace(string(b)))
	}

	return json.NewDecoder(res.Body).Decode(resp)
}

// EncodePeer implements the raft.Transport interface.
func (t *HTTPTransport) EncodePeer(_ raft.ServerID, a raft.ServerAddress) []byte {
	return []byte(a)
}

// DecodePeer implements the raft.Transport interface.
func (t *HTTPTransport) DecodePeer(b []byte) raft.ServerAddress {
	return raft.ServerAddress(string(b))
}

func (t *HTTPTransport) handle(res http.ResponseWriter, req *http.Request, rpc raft.RPC) error {
	if err := json.NewDecoder(req.Body).Decode(&rpc.Command); err != nil {
		err := fmt.Errorf("Could not parse request: %v", err)
		http.Error(res, err.Error(), http.StatusBadRequest)
		return err
	}

	if r, ok := rpc.Command.(*installSnapshotRequest); ok {
		rpc.Command = r.Args
		rpc.Reader = bytes.NewReader(r.Data)
	}

	respChan := make(chan raft.RPCResponse)
	rpc.RespChan = respChan

	t.consumer <- rpc

	resp := <-respChan

	if resp.Error != nil {
		err := fmt.Errorf("Could not run RPC: %v", resp.Error)
		http.Error(res, err.Error(), http.StatusBadRequest)
		return err
	}

	res.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(res).Encode(resp.Response); err != nil {
		err := fmt.Errorf("Could not encode response: %v", err)
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return err
	}

	return nil
}

// ServeHTTP implements the net/http.Handler interface, so that you can use
//
//	http.Handle("/raft/", transport)
func (t *HTTPTransport) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	cmd := path.Base(req.URL.Path)

	var rpc raft.RPC

	switch cmd {
	case "InstallSnapshot":
		rpc.Command = &installSnapshotRequest{}
	case "InstallSnapshotStreaming":
		var isr raft.InstallSnapshotRequest
		if err := json.Unmarshal([]byte(req.Header.Get("X-InstallSnapshotRequest")), &isr); err != nil {
			err := fmt.Errorf("Could not parse request: %v", err)
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}
		rpc.Command = &isr
		rpc.Reader = req.Body
		respChan := make(chan raft.RPCResponse)
		rpc.RespChan = respChan

		t.consumer <- rpc

		resp := <-respChan

		if resp.Error != nil {
			err := fmt.Errorf("Could not run RPC: %v", resp.Error)
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}

		res.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(res).Encode(resp.Response); err != nil {
			err := fmt.Errorf("Could not encode response: %v", err)
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}

		return
	case "RequestVote":
		rpc.Command = &raft.RequestVoteRequest{}
	case "AppendEntries":
		rpc.Command = &raft.AppendEntriesRequest{}
	default:
		http.Error(res, fmt.Sprintf("No RPC %q", cmd), 404)
		return
	}

	if err := t.handle(res, req, rpc); err != nil {
		t.logger.Printf("[%s, %s] %v\n", req.RemoteAddr, cmd, err)
	}
	metrics.IncrCounter([]string{"raft", "httptransport", "handled"}, 1)
}

// SetHeartbeatHandler implements the raft.Transport interface.
func (t *HTTPTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// Not supported
}

// TimeoutNow implements the raft.Transport interface.
func (t *HTTPTransport) TimeoutNow(_ raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return t.send(fmt.Sprintf(t.urlFmt+"TimeoutNow", target), args, resp)
}
