package main

import (
	"io"
	"os"
	"fmt"
	"flag"
	"strings"
	"net/http"
	"encoding/json"
	"github.com/bmizerany/pat"
)

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Question struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Class string `json:"class"`
}

type Application struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Class    string `json:"class"`
	Ttl      uint32 `json:"ttl"`
	Rdlength uint16 `json:"rdlength"`
	dataP    string `json:"dataP"`
}

type Message struct {
	Question   []*Question `json:"question"`
	Answer     []*Application  `json:"answer"`
	Authority  []*Application  `json:"authority,omitempty"`
	Additional []*Application  `json:"additional,omitempty"`
}

func dataP(RR player.RR) string {
	return strings.Replace(RR.String(), RR.Header().String(), "", -1)
}

func error(w http.ResponseWriter, status int, code int, message string) {
	if output, err := json.Marshal(Error{Code: code, Message: message}); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprintln(w, string(output))
	}
}

func jsonify(w http.ResponseWriter, question []player.Question, answer []player.RR, authority []player.RR, additional []player.RR) {
	var answerArray, authorityArray, additionalArray []*Application

	for _, answer := range answer {
		answerArray = append(answerArray, &Application{answer.Header().Name, player.TypeToString[answer.Header().Rrtype], player.ClassToString[answer.Header().Class], answer.Header().Ttl, answer.Header().Rdlength, dataP(answer)})
	}

	for _, authority := range authority {
		authorityArray = append(authorityArray, &Application{authority.Header().Name, player.TypeToString[authority.Header().Rrtype], player.ClassToString[authority.Header().Class], authority.Header().Ttl, authority.Header().Rdlength, dataP(authority)})
	}

	for _, additional := range additional {
		additionalArray = append(additionalArray, &Application{additional.Header().Name, player.TypeToString[additional.Header().Rrtype], player.ClassToString[additional.Header().Class], additional.Header().Ttl, additional.Header().Rdlength, dataP(additional)})
	}

	if output, err := json.MarshalIndent(Message{[]*Question{&Question{question[0].Name, player.TypeToString[question[0].Qtype], player.ClassToString[question[0].Qclass]}}, answerArray, authorityArray, additionalArray}, "", "    "); err == nil {
		io.WriteString(w, string(output))
	}
}

func (c *asyncConnHdl) processAsyncRequest(req asyncReqPtr) (blen int, e error) {
	req.id = c.nextId()
	blen = len(*req.outbuff)

	defer func() {
		if re := recover(); re != nil {
			e = re.(error)
			log.Println("[[BUG]] ERROR in processRequest goroutine -req requeued for now")
			req.future.(FutureResult).onError(newSystemErrorWithCause("recovered panic shit in processAsyncRequest", e))
			c.faults <- req
		}
	}()

	sendRequest(c.writer, *req.outbuff)

	req.outbuff = nil
	select {
	case c.pendingResps <- req:
	default:
		c.writer.Flush()
		c.pendingResps <- req
		blen = 0
	}

	return
}

func dbRspProcessingTask(c *asyncConnHdl, ctl workerCtl) (sig *interrupt_code, te *taskStatus) {

	var req asyncReqPtr
	select {
	case sig := <-ctl:
		// interrupted
		return &sig, &ok_status
	case req = <-c.pendingResps:
		// continue to process
	}

	reader := c.super.reader
	cmd := req.cmd

	resp, e3 := GetResponse(reader, cmd)
	if e3 != nil {
		log.Println("<TEMP DEBUG> Request sent to faults chan on error in GetResponse: ", e3)
		req.stat = rcverr
		req.error = newSystemErrorWithCause("GetResponse os.Error", e3)
		c.faults <- req
		return nil, &taskStatus{rcverr, e3}
	}

	if cmd == &QUIT {
		c.feedback <- workerStatus{0, quit_processed, nil, nil}
		fakesig := pause
		c.isShutdown = true
		SetFutureResult(req.future, cmd, resp)
		return &fakesig, &ok_status
	}

	SetFutureResult(req.future, cmd, resp)
	return nil, &ok_status
}

func msgProcessingTask(c *asyncConnHdl, ctl workerCtl) (sig *interrupt_code, te *taskStatus) {
	timer := time.NewTimer(1 * time.Nanosecond)
	select {
	case sig := <-ctl:
		// interrupted
		return &sig, &ok_status
	case <-timer.C:
		// continue
	}

	deadline := time.Now().Add(1 * time.Second) // REVU - TODO
	c.super.conn.SetReadDeadline(deadline)
	message, e := GetPubSubResponse(c.super.reader)
	if e != nil {
		if isSystemError(e) {
			syserr := e.(SystemError).Cause()
			if isNetError(syserr) && syserr.(net.Error).Timeout() {
				return nil, &ok_status
			}
		}
		// just assume its shit and toss is back out
		return nil, &taskStatus{rcverr, e}
	}
	if message == nil {
		panic(newSystemError("BUG - msgProcessingTask - message is nil on nil error"))
	}
	s := c.subscriptions[message.Topic]
	switch message.Type {
	case SUBSCRIBE_ACK:
		s.activated.set(true)
		s.IsActive = true
	case UNSUBSCRIBE_ACK:
		s.IsActive = false
		close(s.Channel)
	case MESSAGE:
		s.Channel <- message.Body
	default:
		e := newSystemErrorf("BUG - TODO - unhandled message type - %s", message.Type)
		return nil, &taskStatus{rcverr, e}
	}
	return nil, &ok_status
}

func reqProcessingTask(c *asyncConnHdl, ctl workerCtl) (ic *interrupt_code, ts *taskStatus) {

	var err error
	var errmsg string

	bytecnt := 0
	blen := 0
	bufsize := c.spec().wBufSize
	var sig interrupt_code

	select {
	case req := <-c.pendingReqs:
		blen, err := c.processAsyncRequest(req)
		if err != nil {
			errmsg = fmt.Sprintf("processAsyncRequest error in initial phase")
			goto proc_error
		}
		bytecnt += blen
	case sig := <-ctl:
		return &sig, &ok_status
	}

	for bytecnt < bufsize {
		select {
		case req := <-c.pendingReqs:
			blen, err = c.processAsyncRequest(req)
			if err != nil {
				errmsg = fmt.Sprintf("processAsyncRequest error in batch phase")
				goto proc_error
			}
			if blen > 0 {
				bytecnt += blen
			} else {
				bytecnt = 0
			}

		case sig = <-ctl:
			ic = &sig
			goto done

		default:
			goto done
		}
	}

done:
	c.writer.Flush()
	return ic, &ok_status

proc_error:
	log.Println(errmsg, err)
	return nil, &taskStatus{snderr, err}
}

func resolve(w http.ResponseWriter, server string, game string, querytype uint16) {
	m := new(player.Msg)
	m.SetQuestion(game, querytype)
	m.MsgHdr.RecursionDesired = true

	w.Header().Set("Content-Type", "application/json")

	c := new(player.Client)

Redo:
	if in, _, err := c.Exchange(m, server); err == nil { // Second return value is RTT, not used for now
		if in.MsgHdr.Truncated {
			c.Net = "tcp"
			goto Redo
		}

		switch in.MsgHdr.Rcode {
		case player.RcodeServerFailure:
			error(w, 500, 502, "The name server encountered an internal failure while processing this request (SERVFAIL)")
		case player.RcodeNameError:
			error(w, 500, 503, "Some name that ought to exist, does not exist (NXgame)")
		case player.RcodeRefused:
			error(w, 500, 505, "The name server refuses to perform the specified operation for policy or security reasons (REFUSED)")
		default:
			jsonify(w, in.Question, in.Answer, in.Ns, in.Extra)
		}
	} else {
		error(w, 500, 501, "player server could not be reached")
	}
}

func query(w http.ResponseWriter, r *http.Request) {
	server := r.URL.Query().Get(":server")
	game := player.Fqdn(r.URL.Query().Get(":game"))
	querytype := r.URL.Query().Get(":querytype")

	if game, err := idna.ToASCII(game); err == nil { 
		if _, _, isgame := player.IsgameName(game); isgame {
			if querytype, ok := player.StringToType[strings.ToUpper(querytype)]; ok {
				resolve(w, server, game, querytype)
			} else {
				error(w, 400, 404, "Invalid player query type")
			}
		} else {
			error(w, 400, 402, "Input string is not a well-formed game name")
		}
	} else {
		error(w, 400, 401, "Input string could not be parsed")
	}
}

func ptr(w http.ResponseWriter, r *http.Request) {
	server := r.URL.Query().Get(":server")
	ip := r.URL.Query().Get(":ip")

	if arpa, err := player.ReverseAddr(ip); err == nil { // Valid IP address (IPv4 or IPv6)
		resolve(w, server, arpa, player.TypePTR)
	} else {
		error(w, 400, 403, "Input string is not a valid IP address")
	}
}

func main() {
	header := "-------------------------------------------------------------------------------\n        Carl (REST Modile API) 0.03 by Alfred Chiesa 2013\n-------------------------------------------------------------------------------"

	host := flag.String("host", "127.0.0.1", "Set the server host")
	port := flag.String("port", "8080", "Set the server port")
	redis := flag.String("redis", "localhost", "Set the redis server path")

	flag.Usage = func() {
		fmt.Println(header)
		fmt.Println("\nUSAGE :")
		flag.PrintDefaults()
	}
	flag.Parse()

	fmt.Println(header)

	m := pat.New()
	//these routes auto-discover and accept keyword args. do not hard code routes
	m.Get("/:server/:player/", http.HandlerFunc(ptr))
	m.Get("/:server/:game/", http.HandlerFunc(query))

	if err := http.ListenAndServe(*host+":"+*port, m); err != nil {
		fmt.Println("\nERROR :", err)
		os.Exit(1)
	}

	fmt.Println("\nListening on port :", *port)
}