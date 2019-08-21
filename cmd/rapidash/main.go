package main

import (
	"bufio"
	"encoding/json"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/rakyll/statik/fs"
	_ "go.knocknote.io/rapidash/static/statik"
	"golang.org/x/xerrors"
)

type Option struct {
	Log LogCommand `description:"generate HTML file for log sequence graph" command:"log"`
}

var opts Option

type RapidashLog struct {
	ID      string `json:"id"`
	Level   string `json:"level"`
	Command string `json:"command"`
	Type    string `json:"type"`
	Key     string `json:"key"`
	Value   string `json:"value"`
	Time    int64  `json:"time"`
	Message string `json:"message"`
}

type LogCommand struct {
	OutputFileName string `long:"output" short:"o" default:"rapidash.html" description:"output html file name"`
}

// nolint:unparam
func (lc *LogCommand) Execute(args []string) error {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return xerrors.Errorf("failed to get stat for stdin: %w", err)
	}
	if stat.Size() == 0 {
		log.Println("'rapidash log' command requires stdin characters but that size is zero")
		return nil
	}

	rapidashLogs := []*RapidashLog{}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Err() != nil {
			return xerrors.Errorf("failed to scan: %w", err)
		}
		var l RapidashLog
		if err := json.Unmarshal(scanner.Bytes(), &l); err != nil {
			log.Printf("ignore line: %v\n", xerrors.Errorf("failed to unmarshal log: %w", err))
			continue
		}
		if l.ID != "" {
			rapidashLogs = append(rapidashLogs, &l)
		}
	}
	statikFS, err := fs.New()
	if err != nil {
		return xerrors.Errorf("failed to create statik instance: %w", err)
	}
	jsSource, err := lc.readFile(statikFS, "/build.js")
	if err != nil {
		return xerrors.Errorf("failed to read build.js: %w", err)
	}
	logString, err := json.Marshal(rapidashLogs)
	if err != nil {
		return xerrors.Errorf("failed to marshal logs: %w", err)
	}
	source := struct {
		BuildSource template.JS
		RapidashLog template.JS
	}{
		BuildSource: template.JS(jsSource),
		RapidashLog: template.JS(logString),
	}

	tmplData, err := lc.readFile(statikFS, "/rapidash.tmpl")
	if err != nil {
		return xerrors.Errorf("failed to read rapidash.tmpl: %w", err)
	}
	tmpl := template.Must(template.New("rapidash").Parse(string(tmplData)))

	file, err := os.OpenFile(lc.OutputFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return xerrors.Errorf("failed to open %s: %w", lc.OutputFileName, err)
	}
	defer file.Close()
	if err := tmpl.Execute(file, source); err != nil {
		return xerrors.Errorf("failed to execute template: %w", err)
	}

	return nil
}

func (lc *LogCommand) readFile(fs http.FileSystem, fileName string) ([]byte, error) {
	f, err := fs.Open(fileName)
	if err != nil {
		return nil, xerrors.Errorf("failed to open %s: %w", fileName, err)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, xerrors.Errorf("failed to read from %s: %w", fileName, err)
	}
	return data, nil
}

//go:generate statik -src ../../static/dist -p statik -dest ../../static -f -c '' -m
func main() {
	parser := flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		log.Println(xerrors.Errorf("failed to parse flgas: %w", err))
		os.Exit(1)
	}
}
