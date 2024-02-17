package httpd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func statusCodeToError(sc int) error {
	statusText := http.StatusText(sc)
	if statusText == "" {
		statusText = "unknown status code"
	}
	return fmt.Errorf("failed request with: " + statusText)
}

func CliGet(addr string, key string) (string, error) {
	url := "http://" + addr + "/" + "key" + "/" + key

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", statusCodeToError(resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	val := map[string]string{}
	err = json.Unmarshal(body, &val)
	if err != nil {
		return "", err
	}
	return val[key], nil
}

func CliSet(addr string, key string, value string) error {
	url := "http://" + addr + "/" + "key"
	data, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusCodeToError(resp.StatusCode)
	}
	return nil

}

func CliJoin(joinAddr, nodeId, raftAddr string) error {
	url := "http://" + joinAddr + "/" + "join"
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeId})
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusCodeToError(resp.StatusCode)
	}
	return nil
}
