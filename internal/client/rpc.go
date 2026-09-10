package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

type Request struct {
	ID     string         `json:"id"`
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

type Response struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("rpc error %s: %s", e.Code, e.Message)
}

func WriteFrame(w io.Writer, req *Request) error {
	reqByte, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	if _, err := w.Write(append(reqByte, '\n')); err != nil {
		return fmt.Errorf("writing request bytes: %w", err)
	}

	return nil
}

func ReadFrame(reader *bufio.Reader) (json.RawMessage, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	return json.RawMessage(TrimNewline(line)), nil
}

func TrimNewline(line []byte) []byte {
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return line
}
