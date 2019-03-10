package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/jessevdk/go-flags"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"be verbose"`
	Dryrun      bool   `short:"n" long:"dryrun" description:"don't perform the backup, list what would happen"`
	StreamURL   string `short:"s" long:"stream-url" default:"http://localhost:3000" description:"Logjam endpoint for retrieving stream definitions"`
	BackupDir   string `short:"b" long:"backup-dir" default:"." description:"Directory where to store backups"`
	DatabaseURL string `short:"d" long:"database" default:"mongodb://localhost:27017" description:"Mongo DB host to back up"`
}

var (
	rc          = int(0)
	verbose     = false
	dryrun      = false
	interrupted bool
	streams     map[string]stream
)

type stream struct {
	App                       string        `json:"app"`
	Env                       string        `json:"env"`
	RequestCleaningThreshold  time.Duration `json:"request_cleaning_threshold"`
	DatabaseCleaningThreshold time.Duration `json:"database_cleaning_threshold"`
}

func (s *stream) DatabaseHasExpired(date time.Time) bool {
	threshold := time.Now().Add(-s.DatabaseCleaningThreshold * time.Hour * 24)
	return date.Before(threshold)
}

func (s *stream) RequestCollectionHasExpired(date time.Time) bool {
	threshold := time.Now().Add(-s.RequestCleaningThreshold * time.Hour * 24)
	return date.Before(threshold)
}

type databaseInfo struct {
	App  string
	Env  string
	Date time.Time
}

func (i *databaseInfo) StreamName() string {
	return i.App + "-" + i.Env
}

func parseDatabaseName(db string) *databaseInfo {
	re := regexp.MustCompile(`^logjam-([^-]+)-([^-]+)-(\d\d\d\d-\d\d-\d\d)$`)
	matches := re.FindStringSubmatch(db)
	info := &databaseInfo{App: matches[1], Env: matches[2]}
	t, err := time.Parse("2006-01-02", matches[3])
	if err != nil {
		logError("could not parse database date: %s", matches[3])
		return nil
	}
	info.Date = t
	return info
}

func parseBackupName(file string) (*databaseInfo, string) {
	re := regexp.MustCompile(`^logjam-([^-]+)-([^-]+)-(\d\d\d\d-\d\d-\d\d)\.(archive|requests)$`)
	matches := re.FindStringSubmatch(file)
	if len(matches) != 5 {
		// not a backup file
		return nil, ""
	}
	info := &databaseInfo{App: matches[1], Env: matches[2]}
	t, err := time.Parse("2006-01-02", matches[3])
	if err != nil {
		logError("could not parse database date: %s", matches[3])
		return nil, ""
	}
	info.Date = t
	return info, matches[4]
}

func initialize() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Println(err)
		}
		os.Exit(1)
	}
	if len(args) > 1 {
		logError("%s: passing arguments is not supported. please use options instead.", args[0])
		os.Exit(1)
	}
	u, err := url.Parse(opts.StreamURL)
	if err != nil {
		logError("could not parse stream url: %s", err)
		os.Exit(1)
	}
	u.Path = path.Join(u.Path, "admin/streams")
	url := u.String()
	streams = retrieveStreams(url)
	if streams == nil {
		os.Exit(1)
	}
	if _, err := os.Stat(opts.BackupDir); os.IsNotExist(err) {
		logError("backup directory does not exist")
		os.Exit(1)
	}
	verbose = opts.Verbose
	dryrun = opts.Dryrun
}

func retrieveStreams(url string) map[string]stream {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		logError("could not create http request: %s", err)
		return nil
	}
	req.Header.Add("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		logError("could not retrieve streams from %s: %s", url, err)
		return nil
	}
	if res.StatusCode != 200 {
		logError("unexpected response: %d", res.Status)
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logError("could not read response body: %s", err)
		return nil
	}
	defer res.Body.Close()
	var streams map[string]stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		logError("could not parse stream: %s", err)
		return nil
	}
	return streams
}

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		interrupted = true
		signal.Stop(c)
	}()
}

func logInfo(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("INFO %s\n", format)
	fmt.Printf(finalFormat, args...)
}

func logError(format string, args ...interface{}) {
	rc = 1
	finalFormat := fmt.Sprintf("ERROR %s\n", format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

func logWarn(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("WARN %s\n", format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

func getDatabases() []string {
	client, err := mongo.NewClient(options.Client().ApplyURI(opts.DatabaseURL))
	if err != nil {
		logError("could not create client: %s", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		logError("could not connect: %s", err)
		return nil
	}
	names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		logError("could not list databases: %s", err)
		return nil
	}
	databases := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasPrefix(name, "logjam") {
			databases = append(databases, name)
		}
	}
	return databases
}

func backupWithoutRequests(db string) {
	backupName := filepath.Join(opts.BackupDir, db+".archive")
	_, err := os.Stat(backupName)
	if err == nil {
		logInfo("archive already exists: %s", backupName)
		return
	}
	cmd := exec.Command("mongodump", "--excludeCollection=requests", "--archive="+backupName, "--db="+db, "--gzip")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logInfo("creating archive for %s", db)
	if dryrun {
		return
	}
	err = cmd.Run()
	if err != nil {
		logError("creating archive failed: %s", err)
		err := os.Remove(backupName)
		if err != nil {
			logError("could not remove archive %s: %s", backupName, err)
		}
	}
}

func backupRequests(db string) {
	backupName := filepath.Join(opts.BackupDir, db+".requests")
	_, err := os.Stat(backupName)
	if err == nil {
		logInfo("request backup already exists: %s", backupName)
		return
	}
	cmd := exec.Command("mongodump", "--collection=requests", "--archive="+backupName, "--db="+db, "--gzip")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logInfo("backing up requests for %s", db)
	if dryrun {
		return
	}
	err = cmd.Run()
	if err != nil {
		logError("backing up requests failed: %s", err)
		err := os.Remove(backupName)
		if err != nil {
			logError("could not remove request backup file %s: %s", backupName, err)
		}
	}
}

func backupDatabase(db string) {
	if db == "logjam-global" || strings.Index(db, "logjam-development") != -1 {
		return
	}
	info := parseDatabaseName(db)
	if info == nil {
		rc = 1
		return
	}
	streamName := info.StreamName()
	stream, found := streams[streamName]
	if !found {
		logError("could not find stream info: %s", streamName)
		return
	}
	if !stream.DatabaseHasExpired(info.Date) {
		backupWithoutRequests(db)
	}
	if !stream.RequestCollectionHasExpired(info.Date) {
		backupRequests(db)
	}
}

func backupDatabases(dbs []string) {
	for _, db := range dbs {
		if interrupted {
			break
		}
		backupDatabase(db)
	}
}

func removeExpiredBackups() {
	files, err := ioutil.ReadDir(opts.BackupDir)
	if err != nil {
		logError("could not read backup dir: %s", err)
		return
	}
	for _, f := range files {
		name := f.Name()
		info, suffix := parseBackupName(name)
		if info == nil {
			continue
		}
		streamName := info.StreamName()
		stream, found := streams[streamName]
		if !found {
			logError("could not find stream info: %s", streamName)
			continue
		}
		remove := false
		switch suffix {
		case "requests":
			remove = stream.RequestCollectionHasExpired(info.Date)
		case "archive":
			remove = stream.DatabaseHasExpired(info.Date)
		}
		if remove {
			path := filepath.Join(opts.BackupDir, name)
			logInfo("removing archive %s", path)
			err := os.Remove(path)
			if err != nil {
				logError("could not remove archive %s: %s", path, err)
			}
		}
	}
}

func main() {
	initialize()
	logInfo("%s: starting backup", os.Args[0])
	installSignalHandler()
	dbs := getDatabases()
	backupDatabases(dbs)
	removeExpiredBackups()
	logInfo("%s: backup complete", os.Args[0])
	os.Exit(rc)
}
