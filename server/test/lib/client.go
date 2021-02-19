package test

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"strconv"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"golang.org/x/net/http2"
	"github.com/hopkings2008/yigfs/server/helper"
)


const (
	Endpoint = "https://s3.test.com:9088"
	Region = "cn-bj-1"
	BucketName = "test_bucket"
	Generation = 0
	ZoneId = "cd77df31-08c1-407c-a561-4c0341c77fa4"
	Machine = "172.20.13.155"
	Ino = 1
	ParentIno = 1
	FileName = "test.txt"
	Size = 128
	Nlink = 1
	UpdateUid = 20
	UpdatePerm = 755
	Offset = 0
	SegStartAddr = 0
	SegEndAddr = 128
	SegmentId0 = 1
	SegmentId1 = 1
)

func SendHttpToYigFs(method string, newServer string, client *http.Client, reqStr []byte) (result io.ReadCloser, err error) {
	req, err := http.NewRequest(method, newServer, bytes.NewReader(reqStr))
	if err != nil {
		log.Printf("failed to new post http/2 request to server %s, err: %v", newServer, err)
		return nil, err
	}
	req.Header.Add("Content-Length", strconv.Itoa(len(reqStr)))

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("failed to send req to server %s, err: %v", newServer, err)
		return nil, err
	}

	return resp.Body, nil
}

func tlsConfig() *tls.Config {
	crt, err := ioutil.ReadFile(helper.CONFIG.MetaServiceConfig.TlsCertFile)
	if err != nil {
		log.Fatal(err)
	}
 
	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM(crt)
 
	return &tls.Config{
		RootCAs:            rootCAs,
		InsecureSkipVerify: false,
		ServerName:         "localhost",
	}
}

func transport2() *http2.Transport {
	return &http2.Transport {
		TLSClientConfig:     tlsConfig(),
		DisableCompression:  true,
		AllowHTTP:           false,
	}
}

func NewClient() *http.Client {
	client := &http.Client{Transport: transport2()}
	return client
}

func init() {
    	// Setup config
	helper.SetupConfig()
}
