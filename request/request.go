package request

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

const (
	secURL  = "https://data.sec.gov/submissions/CIK"
	wikiURL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
)

type Company struct {
	Name      string   `json:"name"`
	CIK       string   `json:"cik"`
	Tickers   []string `json:"tickers"`
	Exchanges []string `json:"exchanges"`
}

func GetCompany(cik string) (*Company, error) {
	req, err := buildRequest(secURL + cik + ".json")
	if err != nil {
		return nil, err
	}
	res, err := sendRequest(req)
	if err != nil {
		return nil, err
	}
	data, err := handleJSONResponse(res)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetSP() ([]string, error) {
	req, err := buildRequest(wikiURL)
	if err != nil {
		return nil, err
	}
	res, err := sendRequest(req)
	if err != nil {
		return nil, err
	}
	ciks, err := handleHTMLResponse(res)
	if err != nil {
		return nil, err
	}
	return ciks, nil
}

func handleJSONResponse(res *http.Response) (*Company, error) {
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	body := &Company{}
	if err = json.Unmarshal(bodyBytes, &body); err != nil {
		return nil, err
	}
	body.CIK = extendCIK(body.CIK)
	return body, nil
}

func extendCIK(cik string) string {
	if len(cik) < 10 {
		return extendCIK("0" + cik)
	}
	return cik
}

func handleHTMLResponse(res *http.Response) ([]string, error) {
	defer res.Body.Close()
	bytesRes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	document, err := html.Parse(strings.NewReader(string(bytesRes)))
	if err != nil {
		return nil, err
	}
	tables := findTables(document)
	if len(tables) < 1 {
		return nil, errors.New("Couldn't find any tables on source page for ciks")
	}
	return filterElements(tables[0]), nil
}

func filterElements(table *html.Node) []string {
	var ciks []string
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "td" {
			content := parseElement(node)
			if isCIK(content) {
				ciks = append(ciks, content)
			}
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(table)
	return ciks
}

func parseElement(element *html.Node) string {
	var buf bytes.Buffer
	w := io.Writer(&buf)
	html.Render(w, element)
	nodeStr := buf.String()
	if len(nodeStr) < 15 {
		return ""
	}
	return nodeStr[4:14]
}

func isCIK(element string) bool {
	if len(element) != 10 {
		return false
	}
	if _, err := strconv.Atoi(element); err != nil {
		return false
	}
	return true
}

func findTables(document *html.Node) []*html.Node {
	var tables []*html.Node
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "table" {
			tables = append(tables, node)
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(document)
	return tables
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
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
