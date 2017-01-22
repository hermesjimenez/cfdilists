package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// BlobFileProperties : Math Container structure blob azure sat
type BlobFileProperties struct {
	LastModified    string `xml:"Last-Modified"`
	Etag            string
	ContentLength   string `xml:"Content-Length"`
	ContentType     string `xml:"Content-Type"`
	ContentEncoding string `xml:"Content-Encoding"`
	ContentLanguage string `xml:"Content-Language"`
	ContentMD5      string `xml:"Content-MD5"`
	CacheControl    string `xml:"Cache-Control"`
	BlobType        string
	LeaseStatus     string
}

// BlobData : Blob specific file to download from blob azure sat
type BlobData struct {
	Name       string
	Url        string
	Properties BlobFileProperties
}

// BlobElement : Blob specific file to download from blob azure sat
/*type BlobElement struct {
	Blob BlobData
}*/

// BlobContainer : Container for downloading files from blob azure sat
type BlobContainer struct {
	XMLName xml.Name `xml:"EnumerationResults"`
	Prefix  string
	Blobs   []BlobData `xml:"Blobs>Blob"`
}

type cfdilist struct {
	urlBase   string
	Container []BlobContainer
	Hash      string
}

// GetActualContainer : downloading data from blob azure sat
func GetActualContainer(urlBase string) (BlobContainer, error) {
	now := time.Now()
	var result BlobContainer
	//we iterate for last week
	for d := 0; d < 8; d++ {
		actual := now.AddDate(0, 0, -d)
		urlsearch := urlBase + actual.Format("2006_01_02")
		fmt.Println(urlsearch)
		response, err := http.Get(urlsearch)
		if err != nil {
			return result, errors.New("Error searching for container :" + urlsearch)
		}
		defer response.Body.Close()
		responseData, err := ioutil.ReadAll(response.Body)
		//fmt.Println(string(responseData))
		xml.Unmarshal(responseData, &result)
		/*fmt.Printf("%#v/n", result)*/
		if len(result.Blobs) > 4 && result.Prefix != "" {
			fmt.Println("Total Blobs: ", len(result.Blobs))
			for index := 0; index < len(result.Blobs); index++ {
				fmt.Printf("%#v \n\n", result.Blobs[index])
			}
			return result, nil
		}
	}
	return result, nil
}

func downloadFileFromBlob(urlFile string, wg *sync.WaitGroup) {
	now := time.Now()

	tokens := strings.Split(urlFile, "/")
	fileName := tokens[len(tokens)-1]
	fmt.Println("Creating File OS...File:", fileName)
	fileNameOutput, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error while creating file os: ", fileName, " Error :", err)
		return
	}
	fmt.Println("Downloading...File:", fileName)
	defer fileNameOutput.Close()
	response, err := http.Get(urlFile)
	if err != nil {
		fmt.Println("Error while downloading File: ", urlFile, " Error:", err)
		return
	}
	defer response.Body.Close()

	n, err := io.Copy(fileNameOutput, response.Body)
	if err != nil {
		fmt.Println("Error while writing file to disk: ", fileNameOutput, " Error:", err)
		return
	}
	fmt.Println(n, " Bytes Donwloaded: ", fileName)
	elapsed := time.Since(now).Seconds()
	fmt.Println(" Time Elapsed: ", elapsed, " seconds")
	defer wg.Done()
}

// DownloadFromAzureStatic : Download first version
func DownloadFromAzureStatic() {
	now := time.Now()
	var wg sync.WaitGroup
	url := []string{"https://cfdisat.blob.core.windows.net/lco/l_RFC_2017_01_04_1.txt.gz",
		"https://cfdisat.blob.core.windows.net/lco/l_RFC_2017_01_04_2.txt.gz",
		"https://cfdisat.blob.core.windows.net/lco/l_RFC_2017_01_04_3.txt.gz",
		"https://cfdisat.blob.core.windows.net/lco/l_RFC_2017_01_04_4.txt.gz",
		"https://cfdisat.blob.core.windows.net/lco/l_RFC_2017_01_04_5.txt.gz"}

	for index := 0; index < len(url); index++ {
		wg.Add(1)
		go downloadFileFromBlob(url[index], &wg)
	}

	wg.Wait()
	elapsed := time.Since(now).Seconds()
	fmt.Println("Done Downloading Time Elapsed: ", elapsed, " seconds")
}
func main() {
	now := time.Now()
	fmt.Println("Starting at :", now.Format(time.RFC3339))
	container, err := GetActualContainer("https://cfdisat.blob.core.windows.net/lco?restype=container&comp=list&prefix=l_RFC_")
	if err != nil {
		fmt.Println("Error getting container: ", err)
	}
	fmt.Println(container.Prefix)

}
