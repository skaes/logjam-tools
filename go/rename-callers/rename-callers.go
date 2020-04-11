package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/skaes/logjam-tools/go/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var opts struct {
	Verbose     bool   `short:"v" long:"verbose" description:"Be verbose."`
	Quiet       bool   `short:"q" long:"quiet" description:"Only log errors and warnings."`
	Dryrun      bool   `short:"n" long:"dryrun" description:"Don't perform any renaming, list what would happen."`
	BackupDir   string `short:"b" long:"backup-dir" default:"." description:"Directory where to backups are stored."`
	DatabaseURL string `short:"d" long:"database" default:"mongodb://localhost:27017" description:"Mongo DB host:port to restore to."`
	ToDate      string `short:"t" long:"to-date" description:"End date of backup period. Defaults to yesterday."`
	BeforeDate  string `short:"T" long:"before-date" description:"Day after the end date of backup period. Defaults to today."`
	FromDate    string `short:"f" long:"from-date" description:"Start date of backup period. Defaults to zero time."`
	Rename      string `short:"r" long:"rename" default:"" description:"Rename the restored databases using the given map (old:new,...). Defaults to no renaming."`
}

var (
	rc       = int(0)
	verbose  = false
	dryrun   = false
	toDate   time.Time
	fromDate time.Time
	rename   map[string]string
	client   *mongo.Client
	ctx      context.Context
	cancel   context.CancelFunc
	regex    *regexp.Regexp
)

// IsoDateFormat is used to parse/print iso date parts.
const IsoDateFormat = "2006-01-02"

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
		logError("arguments are not supported")
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
	rename = make(map[string]string, 0)
	mappings := strings.Split(opts.Rename, ",")
	keys := ""
	for _, pair := range mappings {
		if pair != "" {
			split := strings.Split(pair, ":")
			from, to := split[0], split[1]
			if from == "" || to == "" {
				logError("could not parse renaming: %s", pair)
				os.Exit(1)
			}
			rename[from] = to
			if keys != "" {
				keys += "|"
			}
			keys += from
		}
	}
	if len(rename) == 0 {
		logError("you need to specify at least one renaming pair using the --rename option")
		os.Exit(1)
	}
	regex = regexp.MustCompile("^(" + keys + ")[@-](.*)$")
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

type databaseInfo struct {
	App        string
	Env        string
	Date       time.Time
	Name       string
	StreamName string
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

func renameCallersAndSendersInCollection(db *mongo.Database, collectionName string) bool {
	collection := db.Collection(collectionName)
	filter := bson.D{{"$or",
		bson.A{
			bson.D{{"callers", bson.D{{"$exists", 1}}}},
			bson.D{{"senders", bson.D{{"$exists", 1}}}},
		}},
	}
	projection := bson.D{
		{"callers", 1},
		{"senders", 1},
	}
	cursor, err := collection.Find(context.Background(), filter, options.Find().SetProjection(projection))
	if err != nil {
		logError("could not retrieve documents: %s", err)
		return false
	}
	var operations []mongo.WriteModel
	for cursor.Next(context.Background()) {
		var doc bson.M
		cursor.Decode(&doc)
		deletions := make(bson.M)
		increments := make(bson.M)
		if callers := doc["callers"]; callers != nil {
			for k, v := range callers.(bson.M) {
				matches := regex.FindStringSubmatch(k)
				if len(matches) == 3 {
					from := matches[1]
					to := rename[from]
					deletions["callers."+k] = 1
					increments["callers."+to+"@"+matches[2]] = v
				}
			}
		}
		if senders := doc["senders"]; senders != nil {
			for k, v := range senders.(bson.M) {
				matches := regex.FindStringSubmatch(k)
				if len(matches) == 3 {
					from := matches[1]
					to := rename[from]
					deletions["senders."+k] = 1
					increments["senders."+to+"@"+matches[2]] = v
				}
			}
		}
		if len(deletions) > 0 {
			operation := mongo.NewUpdateOneModel()
			operation.SetFilter(bson.D{{"_id", doc["_id"]}})
			operation.SetUpdate(bson.D{{"$inc", increments}, {"$unset", deletions}})
			operations = append(operations, operation)
		}
	}
	if len(operations) == 0 {
		return false
	}
	if dryrun {
		return true
	}
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)
	_, err = collection.BulkWrite(context.TODO(), operations, &bulkOption)
	if err != nil {
		logError("bulk update failed:", err)
		return false
	}
	return true
}

func renameCallerAndSendersInDatabase(di *databaseInfo) bool {
	db := client.Database(di.Name)
	renamedTotals := renameCallersAndSendersInCollection(db, "totals")
	renamedMinutes := renameCallersAndSendersInCollection(db, "minutes")
	if renamedTotals || renamedMinutes {
		logInfo("renamed caller and sender references in %s", di.Name)
		return true
	}
	logInfo("nothing renamed in %s", di.Name)
	return false
}

func renameCallersAndSenders(databases []*databaseInfo) {
	for _, di := range databases {
		if di.Date.Before(fromDate) || di.Date.After(toDate) {
			continue
		}
		if !renameCallerAndSendersInDatabase(di) || dryrun {
			continue
		}
		backupName := filepath.Join(opts.BackupDir, di.Name+".archive")
		_, err := os.Stat(backupName)
		if err == nil {
			baseName := filepath.Base(backupName)
			err := os.Remove(backupName)
			if err != nil {
				logError("could not remove archive %s: %s", baseName, err)
			} else {
				logInfo("removed archive: %s", baseName)
			}
		}
	}
}

func main() {
	initialize()
	logInfo("%s: starting renaming in %s", os.Args[0], opts.BackupDir)
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
	databases := getDatabases()
	renameCallersAndSenders(databases)
	logInfo("%s: renaming complete", os.Args[0])
	os.Exit(rc)
}
