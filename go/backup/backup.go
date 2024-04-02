package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/skaes/logjam-tools/go/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var opts struct {
	Verbose             bool   `short:"v" long:"verbose" description:"Be verbose."`
	Quiet               bool   `short:"q" long:"quiet" description:"Only log errors and warnings."`
	Force               bool   `short:"x" long:"force" description:"Perform backup even if file already exists."`
	Dryrun              bool   `short:"n" long:"dryrun" description:"Don't perform the backup, list what would happen."`
	StreamURL           string `short:"s" long:"stream-url" default:"http://localhost:3000" description:"Logjam endpoint for retrieving stream definitions."`
	BackupDir           string `short:"b" long:"backup-dir" default:"." description:"Directory where to store backups."`
	DatabaseURL         string `short:"d" long:"database" default:"mongodb://localhost:27017" description:"Mongo DB host to back up."`
	ToDate              string `short:"t" long:"to-date" description:"End date of backup period. Defaults to yesterday."`
	BeforeDate          string `short:"T" long:"before-date" description:"Day after the end date of backup period. Defaults to today."`
	FromDate            string `short:"f" long:"from-date" description:"Start date of backup period. Defaults to zero time."`
	Pattern             string `short:"m" long:"match" default:".*" description:"Restrict backup to database names matching the given regexp."`
	ParallelCollections uint   `short:"p" long:"collections" default:"4" description:"Number of collections restored in parallel. Defaults to 4."`
}

var (
	rc               = int(0)
	verbose          = false
	dryrun           = false
	streams          map[string]stream
	toDate           time.Time
	fromDate         time.Time
	pattern          *regexp.Regexp
	client           *mongo.Client
	ctx              context.Context
	cancel           context.CancelFunc
	scheduledBackups map[string]bool
)

// IsoDateFormat is used to parse/print iso date parts.
const IsoDateFormat = "2006-01-02"

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
	App        string
	Env        string
	Date       time.Time
	Name       string
	StreamName string
}

func parseDatabaseName(db string) *databaseInfo {
	re := regexp.MustCompile(`^logjam-(.+)-([^-]+)-(\d\d\d\d-\d\d-\d\d)$`)
	matches := re.FindStringSubmatch(db)
	info := &databaseInfo{App: matches[1], Env: matches[2], Name: db}
	info.StreamName = info.App + "-" + info.Env
	t, err := time.Parse(IsoDateFormat, matches[3])
	if err != nil {
		logError("could not parse database date: %s", matches[3])
		return nil
	}
	info.Date = t.Truncate(24 * time.Hour)
	return info
}

func parseBackupName(file string) (*databaseInfo, string) {
	re := regexp.MustCompile(`^logjam-(.+)-([^-]+)-(\d\d\d\d-\d\d-\d\d)\.(archive|requests)$`)
	matches := re.FindStringSubmatch(file)
	if len(matches) != 5 {
		// not a backup file
		return nil, ""
	}
	info := &databaseInfo{App: matches[1], Env: matches[2]}
	info.StreamName = info.App + "-" + info.Env
	t, err := time.Parse("2006-01-02", matches[3])
	if err != nil {
		logError("could not parse database date: %s", matches[3])
		return nil, ""
	}
	info.Date = t.Truncate(24 * time.Hour)
	return info, matches[4]
}

func initialize() {
	args, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		e := err.(*flags.Error)
		if e.Type != flags.ErrHelp {
			fmt.Fprintln(os.Stderr, err)
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
	if opts.ToDate != "" && opts.BeforeDate != "" {
		logError("you can only specify one of --before-date or --to-date")
		os.Exit(1)
	}
	if opts.ToDate != "" {
		t, err := time.Parse(IsoDateFormat, opts.ToDate)
		if err != nil {
			logError("could not parse to-date: %s. Error: %s", opts.ToDate, err)
			os.Exit(1)
		}
		toDate = t
	} else if opts.BeforeDate != "" {
		t, err := time.Parse(IsoDateFormat, opts.BeforeDate)
		if err != nil {
			logError("could not parse before-date: %s. Error: %s", opts.BeforeDate, err)
			os.Exit(1)
		}
		toDate = t.AddDate(0, 0, -1)
	} else {
		// yesterday at the beginning of the day
		toDate = time.Now().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	}
	if opts.FromDate != "" {
		t, err := time.Parse(IsoDateFormat, opts.FromDate)
		if err != nil {
			logError("could not parse from-date: %s. Error: %s", opts.FromDate, err)
			os.Exit(1)
		}
		fromDate = t.Truncate(24 * time.Hour)
	}
	p, err := regexp.Compile(opts.Pattern)
	if err != nil {
		logError("specified pattern '%s' did not compile: %s", opts.Pattern, err)
	}
	pattern = p
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
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		logError("unexpected response: %d", res.Status)
		return nil
	}
	if err != nil {
		logError("could not read response body: %s", err)
		return nil
	}
	var streams map[string]stream
	err = json.Unmarshal(body, &streams)
	if err != nil {
		logError("could not parse stream: %s", err)
		return nil
	}
	return streams
}

func logInfo(format string, args ...interface{}) {
	if !opts.Quiet {
		finalFormat := fmt.Sprintf("%s INFO %s\n", time.Now().Format(time.StampMicro), format)
		fmt.Printf(finalFormat, args...)
	}
}

func logError(format string, args ...interface{}) {
	rc = 1
	finalFormat := fmt.Sprintf("%s ERROR %s\n", time.Now().Format(time.StampMicro), format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

func logWarn(format string, args ...interface{}) {
	finalFormat := fmt.Sprintf("%s WARN %s\n", time.Now().Format(time.StampMicro), format)
	fmt.Fprintf(os.Stderr, finalFormat, args...)
}

func getDatabases() []*databaseInfo {
	names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		logError("could not list databases: %s", err)
		return nil
	}
	dbs := make([]*databaseInfo, 0, len(names))
	for _, name := range names {
		if name == "logjam-global" || !strings.HasPrefix(name, "logjam-") {
			continue
		}
		info := parseDatabaseName(name)
		if info == nil {
			continue
		}
		dbs = append(dbs, info)
	}
	sort.Slice(dbs, func(i, j int) bool {
		younger := dbs[i].Date.Before(dbs[j].Date)
		sameDate := dbs[i].Date == dbs[j].Date
		return younger || (sameDate && strings.Compare(dbs[i].StreamName, dbs[j].StreamName) == -1)
	})
	return dbs
}

type backupKind bool

const (
	backupAlways      backupKind = true
	backupIfNotExists backupKind = false
)

func backupWithoutRequests(db string, kind backupKind) {
	backupName := filepath.Join(opts.BackupDir, db+".archive")
	scheduled := scheduledBackups[db]
	if !scheduled && kind == backupIfNotExists {
		_, err := os.Stat(backupName)
		if err == nil {
			if verbose {
				logInfo("archive already exists: %s", backupName)
			}
			if !opts.Force {
				return
			}
		}
	}
	uri := strings.TrimSuffix(opts.DatabaseURL, "/") + "/" + db
	cmd := exec.Command("mongodump", "--uri="+uri, "--excludeCollection=metrics", "--excludeCollection=requests", "--archive="+backupName, "--gzip")
	if !verbose {
		cmd.Args = append(cmd.Args, "--quiet")
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logInfo("creating archive for %s", db)
	if verbose {
		logInfo("running cmd: %s", strings.Join(cmd.Args, " "))
	}
	if dryrun {
		return
	}
	err := cmd.Run()
	if err != nil {
		logError("failed to create archive %s: %s", backupName, err)
		err = os.Remove(backupName)
		if err != nil {
			logError("could not remove archive %s: %s", filepath.Base(backupName), err)
		}
		return
	}
	if scheduled {
		err := unscheduleBackup(db)
		if err != nil {
			logError("could not remove scheduled backup from the schedule: %s", err)
		}
	}
}

func backupRequests(db string) {
	backupName := filepath.Join(opts.BackupDir, db+".requests")
	scheduled := scheduledBackups[db]
	if !scheduled {
		_, err := os.Stat(backupName)
		if err == nil {
			if verbose {
				logInfo("request backup already exists: %s", filepath.Base(backupName))
			}
			if !opts.Force {
				return
			}
		}
	}
	uri := strings.TrimSuffix(opts.DatabaseURL, "/") + "/" + db
	cmd := exec.Command("mongodump", "--uri="+uri, "--archive="+backupName, "--gzip", "--numParallelCollections="+strconv.Itoa(int(opts.ParallelCollections)))
	for _, s := range []string{"totals", "minutes", "quants", "agents", "heatmaps", "js_exceptions", "events"} {
		cmd.Args = append(cmd.Args, "--excludeCollection="+s)
	}
	if !verbose {
		cmd.Args = append(cmd.Args, "--quiet")
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logInfo("backing up requests for %s", db)
	if verbose {
		logInfo("running cmd: %s", strings.Join(cmd.Args, " "))
	}
	if dryrun {
		return
	}
	err := cmd.Run()
	if err != nil {
		logError("failed tp backup requests %s: %s", backupName, err)
		err = os.Remove(backupName)
		if err != nil {
			logError("could not remove request backup file %s: %s", filepath.Base(backupName), err)
		}
		return
	}
	if scheduled {
		err := unscheduleBackup(db)
		if err != nil {
			logError("could not remove scheduled backup from the schedule: %s", err)
		}
	}
}

func backupDatabase(db *databaseInfo) {
	if db.Name == "logjam-global" || strings.Index(db.Name, "logjam-development") != -1 {
		return
	}
	if db.Date.Before(fromDate) || db.Date.After(toDate) {
		return
	}
	if !pattern.MatchString(db.Name) {
		return
	}
	stream, found := streams[db.StreamName]
	if !found {
		logWarn("could not find stream for database %s, it should probably be deleted", db.Name)
		return
	}
	if !stream.DatabaseHasExpired(db.Date) {
		backupWithoutRequests(db.Name, backupIfNotExists)
	}
	if !stream.RequestCollectionHasExpired(db.Date) {
		backupRequests(db.Name)
	}
}

func backupDatabases(dbs []*databaseInfo) {
	for _, db := range dbs {
		if util.Interrupted() {
			break
		}
		backupDatabase(db)
	}
}

func removeExpiredBackups() {
	files, err := os.ReadDir(opts.BackupDir)
	if err != nil {
		logError("could not read backup dir: %s", err)
		return
	}
	for _, f := range files {
		name := f.Name()
		db, suffix := parseBackupName(name)
		if db == nil {
			continue
		}
		stream, found := streams[db.StreamName]
		if !found {
			logWarn("could not find stream info: '%s'", db.StreamName)
			logWarn("please remove manually: '%s'", name)
			continue
		}
		remove := false
		switch suffix {
		case "requests":
			remove = stream.RequestCollectionHasExpired(db.Date)
		case "archive":
			remove = stream.DatabaseHasExpired(db.Date)
		}
		if remove {
			path := filepath.Join(opts.BackupDir, name)
			logInfo("removing archive %s", name)
			err := os.Remove(path)
			if err != nil {
				logError("could not remove archive %s: %s", name, err)
			}
		}
	}
}

func getScheduledBackups() {
	scheduledBackups = make(map[string]bool)
	metadata := client.Database("logjam-global").Collection("metadata")
	var doc struct {
		Name  string   `bson:"name"`
		Value []string `bson:"value"`
	}
	err := metadata.FindOne(context.Background(), bson.D{{"name", "scheduled-backups"}}).Decode(&doc)
	if err != nil {
		logWarn("could not retrieve/decode scheduled backups: %s", err)
		return
	}
	for _, db := range doc.Value {
		scheduledBackups[db] = true
	}
}

func unscheduleBackup(db string) error {
	delete(scheduledBackups, db)
	metadata := client.Database("logjam-global").Collection("metadata")
	_, err := metadata.UpdateOne(
		context.Background(),
		bson.D{{"name", "scheduled-backups"}},
		bson.D{{"$pull", bson.D{{"value", db}}}},
	)
	return err
}

func cleanScheduledBackups() {
	for dbName := range scheduledBackups {
		db := parseDatabaseName(dbName)
		if db == nil {
			logInfo("removing invalid database name from scheduled backup: %s", dbName)
			if err := unscheduleBackup(dbName); err != nil {
				logError("could not remove invalid scheduled backup from the schedule: %s", err)
			}
			continue
		}
		stream, found := streams[db.StreamName]
		if !found || stream.DatabaseHasExpired(db.Date) {
			logInfo("removing expired scheduled backup: %s", dbName)
			if err := unscheduleBackup(dbName); err != nil {
				logError("could not remove expired scheduled backup from the schedule: %s", err)
			}
		}
	}
}

func main() {
	initialize()
	logInfo("%s: starting backup in %s", os.Args[0], opts.BackupDir)
	util.InstallSignalHandler()

	var err error
	client, err = mongo.NewClient(options.Client().ApplyURI(opts.DatabaseURL))
	if err != nil {
		logError("could not create client: %s", err)
		os.Exit(1)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		logError("could not connect: %s", err)
		os.Exit(1)
	}
	defer client.Disconnect(ctx)

	getScheduledBackups()
	dbs := getDatabases()
	backupDatabases(dbs)
	if !dryrun {
		removeExpiredBackups()
		cleanScheduledBackups()
	}
	logInfo("%s: backup complete", os.Args[0])
	os.Exit(rc)
}
