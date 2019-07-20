package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type (
	DataFile struct {
		Url  string
		Data *os.File
	}
)

var (
	TOKEN         = ""
	STORAGE_URL   = ""
	MapExist      = make(map[string]bool)
	chUploadFiles = make(chan int, 10)
	iNext         = 0
)


func hash(line string) string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(line)))
}

func main() {
	TOKEN, STORAGE_URL = authSelectel()
	if len(os.Args) < 2 || os.Args[2] == "" {
		println("[Error]: Was don't added log file")
		os.Exit(0)
	}
	fResultLog, errOpen := os.Open("./" + os.Args[2])
	if errOpen != nil {
		println("[ErrorOpen]:", errOpen)
	}

	scan := bufio.NewScanner(fResultLog)
	generateMapExistFile := func() {
		for scan.Scan() {
			line := scan.Text()
			arr := strings.Split(line, "/")
			arrFinal := strings.Split(arr[0], ":")
			if arrFinal[0] == "[Success]" {
				MapExist[hash(line)] = true
			}
		}
	}
	generateMapExistFile()
	println("Successfull generated map")

	if len(STORAGE_URL) > 0 && len(TOKEN) > 0 {
		println("start...")

		pwd, _ := os.Getwd()
		searchDir := strings.Join([]string{pwd, os.Args[1]}, "/")

		err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
			switch f.Name() {
			case ".git", "temp", "cache", "managed_cache":
				return filepath.SkipDir
			case "mysql_debug.sql", "match.mp4", "match.webm", "20-03-19_16_21.sql":
				return nil
			}
			if !f.IsDir() {
				data, errOpen := os.Open(path)
				if errOpen != nil {
					fmt.Println("[ErrorOpen]:", errOpen)
					return nil
				}
				re := regexp.MustCompile(pwd + `/(.*)`)
				p := re.FindSubmatch([]byte(path))
				url := strings.Join([]string{STORAGE_URL, "backup_21_06_19", string(p[1])}, "/")
				hashURL := hash(fmt.Sprintf("[Success]: " + url))
				fmt.Println("[URL]:", url)
				iNext++
				chUploadFiles <- iNext
				if _, ok := MapExist[hashURL]; !ok {
					if f.Size() < 1000000 {
						go func() {
							if errUpload := upload(url, data); errUpload == nil {
								if i, ok := <-chUploadFiles; ok {
									fmt.Println("[Async]:", i)
								}
							} else {
								os.Exit(2)
							}
						}()
					} else {
						//sync upload
						upload(url, data)
						if i, ok := <-chUploadFiles; ok {
							fmt.Println("[Sync]:", i)
						}
					}
				} else {
					data.Close()
					fmt.Println("[Miss]:", url)
					if i, ok := <-chUploadFiles; ok {
						fmt.Println("[Miss]:", i)
					}
				}
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
		}

		for {
			time.Sleep(time.Millisecond * 200)
		}

	} else {
		fmt.Printf("\n[Error]: \nTOKEN is %s \nSTORAGE_URL is %s", TOKEN, STORAGE_URL)
	}
}

func authSelectel() (string, string) {
	client := &http.Client{}

	url := "https://api.selcdn.ru/auth/v1.0"
	login := os.Getenv("X_AUTH_USER")
	password := os.Getenv("X_AUTH_KEY")
	req, errRequest := http.NewRequest("GET", url, nil)
	if errRequest != nil {
		fmt.Println("REQUEST-->", errRequest)
	}
	req.Header.Add("X-Auth-User", login)
	req.Header.Add("X-Auth-Key", password)
	resp, errResp := client.Do(req)
	if errResp != nil {
		fmt.Println("RESP-->", errResp)
	}
	if resp.StatusCode == 204 || resp.StatusCode == 200 {
		return resp.Header.Get("X-Auth-Token"), resp.Header.Get("X-Storage-Url")
	}

	return "", ""
}

func uploadSelectel(url string, data *os.File) error {
	client := &http.Client{}

	req, errRequest := http.NewRequest("PUT", url, data)
	if errRequest != nil {
		fmt.Println(errRequest)
		return errRequest
	}
	req.Header.Add("X-Auth-Token", TOKEN)
	resp, errResp := client.Do(req)
	if errResp != nil {
		fmt.Println(errResp)
		fmt.Println("[Error]:", url)
		return errResp
	} else if resp.StatusCode == 201 {
		fmt.Println("[Success]:", url)
	} else if resp.StatusCode == 401 {
		fmt.Printf("[Error]: status code is %d\n", resp.StatusCode)
		return errResp
	} else {
		fmt.Printf("[Error]: status code is %d\n", resp.StatusCode)
		return errResp
	}

	return nil
}

func upload(url string, data *os.File) error {
	secondTry := false
SECOND_TRY:
	if errUpload := uploadSelectel(url, data); errUpload != nil {
		fmt.Printf("[Error]: %s", errUpload)
		if !secondTry {
			TOKEN, STORAGE_URL = authSelectel()
			fmt.Printf("[Reconnect]: Token - %s, Storage - %s\n", TOKEN, STORAGE_URL)
			secondTry = true
			goto SECOND_TRY
		}
		return errUpload
	}
	defer data.Close()
	return nil
}
