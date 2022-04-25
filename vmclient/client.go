package vmclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"errors"
)

type Client struct {
	vmendpoint string
	client     *http.Client
}

// standard prometheus response in case of no errors from the server
// {
//   "status": "success" | "error",
//   "data": <data>,

//   Only set if status is "error". The data field may still hold
//   additional data.
//   "errorType": "<string>",
//   "error": "<string>",

//   Only if there were warnings while executing the request.
//   There will still be data in the data field.
//   "warnings": ["<string>"]
// }
type response struct {
	Status   string          `json:"status"`
	Data     json.RawMessage `json:"data"`
	ErrType  string          `json:"errorType"`
	Err      string          `json:"error"`
	Warnings []string        `json:"warnings"`
}

func NewClient(vmendpoint string) (*Client, error) {
	if vmendpoint == "" {
		return nil, errors.New("host is initialized as empty. giving up")
	}
	_, err := url.Parse(vmendpoint)
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize vmclient: %w", err)
	}
	return &Client{
		vmendpoint: vmendpoint,
		client:     http.DefaultClient,
	}, nil
}

func (vmc *Client) RangeQuery(ctx context.Context, query map[string]string) ([]byte, error) {
	return vmc.doQuery(ctx, "/api/v1/query_range", query)
}

func (vmc *Client) Query(ctx context.Context, query map[string]string) ([]byte, error) {
	return vmc.doQuery(ctx, "/api/v1/query", query)
}

func (vmc *Client) Series(ctx context.Context, query map[string]string) ([]byte, error) {
	return vmc.series(ctx, "/api/v1/series", query)
}

func (vmc *Client) series(ctx context.Context, path string, query map[string]string) ([]byte, error) {
	u, err := url.Parse(vmc.vmendpoint + path)
	if err != nil {
		return nil, fmt.Errorf("unable to parse vm URL: %v", err)
	}

	q := u.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	res, err := vmc.do(req)
	if err != nil {
		return nil, err
	}

	if res.Status == "error" {
		return nil, fmt.Errorf("vmclient: receive error from server: %v ,error type: %v", res.Err, res.ErrType)
	}
	return res.Data, nil
	// res, err := ioutil.ReadFile("/home/last9/series-final.json")
	// if err != nil {
	// 	return nil, err
	// }
	// return res, nil
}

func (vmc *Client) doQuery(ctx context.Context, path string, query map[string]string) ([]byte, error) {
	u, err := url.Parse(vmc.vmendpoint + path)
	if err != nil {
		return nil, fmt.Errorf("unable to parse vm URL: %v", err)
	}

	q := u.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	res, err := vmc.do(req)
	if err != nil {
		return nil, err
	}

	if res.Status == "error" {
		return nil, fmt.Errorf("vmclient: receive error from server: %v ,error type: %v", res.Err, res.ErrType)
	}
	return res.Data, nil
}

func (vmc *Client) do(req *http.Request) (response, error) {
	resp, err := vmc.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}

	var res response
	err = json.Unmarshal(b, &res)
	if err != nil {
		return res, err
	}
	return res, nil
}
