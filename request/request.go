package request

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

const (
	urlStr = "https://data.sec.gov/submissions/CIK"
)

type Company struct {
	Name      string   `json:"name"`
	CIK       string   `json:"cik"`
	Tickers   []string `json:"tickers"`
	Exchanges []string `json:"exchanges"`
}

func GetCompany(cik string) (*Company, error) {
	req, err := buildRequest(urlStr + cik + ".json")
	if err != nil {
		return nil, err
	}
	res, err := sendRequest(req)
	if err != nil {
		return nil, err
	}
	data, err := handleResponse(res)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func handleResponse(res *http.Response) (*Company, error) {
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	company := &Company{}
	if err = json.Unmarshal(bodyBytes, &company); err != nil {
		return nil, err
	}
	company.CIK = extendCIK(company.CIK)
	return company, nil
}

func extendCIK(cik string) string {
	if len(cik) < 10 {
		return extendCIK("0" + cik)
	}
	return cik
}

func buildRequest(urlStr string) (*http.Request, error) {
	req, err := http.NewRequest("GET", urlStr, nil)
	req.Header.Add("User-Agent", "example.com info@example.com")
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Connection", "keep-alive")
	if err != nil {
		return nil, err
	}
	return req, nil
}

func sendRequest(req *http.Request) (*http.Response, error) {
	time.Sleep(200 * time.Millisecond)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
