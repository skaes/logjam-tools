package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/skaes/logjam-tools/go/util"
)

var opts struct {
	Verbose             bool   `short:"v" long:"verbose" description:"Be verbose."`
	Quiet               bool   `short:"q" long:"quiet" description:"Only log errors and warnings."`
	Dryrun              bool   `short:"n" long:"dryrun" description:"Don't perform the backup, list what would happen."`
	BackupDir           string `short:"b" long:"backup-dir" default:"." description:"Directory where to backups are stored."`
	DatabaseURL         string `short:"d" long:"database" default:"mongodb://localhost:27017" description:"Mongo DB host:port to restore to."`
	ExcludeRequests     bool   `short:"x" long:"exclude-requests" description:"Don't restore request backups."`
	ToDate              string `short:"t" long:"to-date" description:"End date of restore period. Defaults to yesterday."`
	BeforeDate          string `short:"T" long:"before-date" description:"Day after the end date of the restore period. Defaults to today."`
	FromDate            string `short:"f" long:"from-date" description:"Start date of restore period. Defaults to zero time."`
	Match               string `short:"m" long:"match" default:"*" description:"Restrict restore to files matching the given file glob. Ignored when an explicit list of archives is given. Defaults to all files."`
	Rename              string `short:"r" long:"rename" default:"" description:"Rename the restored databases using the given map (old:new,...). Defaults to no renaming."`
	Concurrency         uint   `short:"c" long:"concurrency" default:"1" description:"Run this many restore jobs concurrently. Defaults to 1."`
	InsertionWorkers    uint   `short:"w" long:"workers" default:"1" description:"Number of collection insertion workers. Defaults to 1."`
	ParallelCollections uint   `short:"p" long:"collections" default:"4" description:"Number of collections restored in parallel. Defaults to 4."`
}

var (
	rc                = int(0)
	// verbose           = false
	dryrun            = false
	toDate            time.Time
	fromDate          time.Time
	archivesToRestore []string
	rename            map[string]string
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
	archivesToRestore = args[1:]
	if _, err := os.Stat(opts.BackupDir); os.IsNotExist(err) {
		logError("backup directory does not exist")
		os.Exit(1)
	}
	// verbose = opts.Verbose
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
	for _, pair := range mappings {
		if pair != "" {
			split := strings.Split(pair, ":")
			from, to := split[0], split[1]
			if from == "" || to == "" {
				logError("could not parse renaming: %s", pair)
				os.Exit(1)
			}
			rename[from] = to
		}
	}
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

func restoreArchive(ai *archiveInfo) {
	_, err := os.Stat(ai.Path)
	if err != nil {
		logInfo("could not access archive %s: %s", ai.Path, err)
		return
	}
	args := []string{
		"--drop", "--uri=" + opts.DatabaseURL, "--archive=" + ai.Path, "--gzip",
		"--numParallelCollections=" + strconv.Itoa(int(opts.ParallelCollections)),
		"--numInsertionWorkersPerCollection=" + strconv.Itoa(int(opts.InsertionWorkers)),
	}
	newName := rename[ai.App]
	if newName != "" {
		fromName := ai.DatabaseName("")
		toName := ai.DatabaseName(newName)
		args = append(args, "--nsFrom="+fromName+".*", "--nsTo="+toName+".*")
	}
	cmd := exec.Command("mongorestore", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logInfo("restoring archive %s", ai.Path)
	if opts.Verbose || dryrun {
		logInfo("mongorestore %s", strings.Join(args, " "))
	}
	if dryrun {
		return
	}
	err = cmd.Run()
	if err != nil {
		logError("restoring archive %s failed: %s", ai.Path, err)
	}
}

func parseBackupFileName(path string) (string, string) {
	for _, s := range []string{".archive", ".requests"} {
		f := strings.TrimSuffix(path, s)
		if f != path {
			return f, s
		}
	}
	return "", ""
}

func parseArchiveInfo(path string) (string, string, time.Time, error) {
	r := regexp.MustCompile(`^logjam-(.+)-([^-]+)-(\d\d\d\d-\d\d-\d\d)\.(archive|requests)$`)
	matches := r.FindStringSubmatch(filepath.Base(path))
	if len(matches) == 5 {
		t, err := time.Parse(IsoDateFormat, matches[3])
		return matches[1], matches[2], t, err
	}
	return "", "", time.Time{}, errors.New("could not parse archive name")
}

type archiveInfo struct {
	Path string
	App  string
	Env  string
	Date time.Time
}

func (ai *archiveInfo) DatabaseName(name string) string {
	if name == "" {
		name = ai.App
	}
	return fmt.Sprintf("logjam-%s-%s-%s", name, ai.Env, ai.Date.Format(IsoDateFormat))
}

func extractArchiveInfo(a string) *archiveInfo {
	file := filepath.Base(a)
	app, env, date, err := parseArchiveInfo(file)
	if err != nil {
		logError("Ignoring archive '%s': %s", file, err)
		return nil
	}
	return &archiveInfo{App: app, Env: env, Date: date, Path: a}
}

func filterArchives(archives []*archiveInfo) []*archiveInfo {
	res := []*archiveInfo{}
	for _, a := range archives {
		name, suffix := parseBackupFileName(a.Path)
		if name == "" {
			continue
		}
		if a.Date.Before(fromDate) || a.Date.After(toDate) {
			continue
		}
		if !opts.ExcludeRequests || suffix == ".archive" {
			res = append(res, a)
		}
	}
	return res
}

func sortArchives(archives []*archiveInfo) {
	sort.Slice(archives, func(i int, j int) bool {
		younger := archives[i].Date.Before(archives[j].Date)
		sameDate := archives[i].Date == archives[j].Date
		return younger || (sameDate && strings.Compare(archives[i].Path, archives[j].Path) == -1)
	})
}

func restoreArchives(c chan *archiveInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for !util.Interrupted() {
		select {
		case a := <-c:
			restoreArchive(a)
		default:
			return
		}
	}
}

func restoreFromBackups() {
	if len(archivesToRestore) == 0 {
		files, err := filepath.Glob(filepath.Join(opts.BackupDir, opts.Match))
		if err != nil {
			logError("could not read backup dir: %s", err)
			return
		}
		archivesToRestore = files
	} else if opts.Match != "*" {
		logWarn("ignoring match parameter, because an explicit list of files has been given")
	}
	archives := []*archiveInfo{}
	for _, a := range archivesToRestore {
		info := extractArchiveInfo(a)
		if info != nil {
			archives = append(archives, info)
		}
	}
	archives = filterArchives(archives)
	sortArchives(archives)
	c := make(chan *archiveInfo, len(archives))
	for _, a := range archives {
		c <- a
	}
	var wg sync.WaitGroup
	wg.Add(int(opts.Concurrency))
	for i := 0; i < int(opts.Concurrency); i++ {
		go restoreArchives(c, &wg)
	}
	wg.Wait()
}

func main() {
	initialize()
	logInfo("%s: starting restore in %s", os.Args[0], opts.BackupDir)
	util.InstallSignalHandler()
	restoreFromBackups()
	logInfo("%s: restore complete", os.Args[0])
	os.Exit(rc)
}
