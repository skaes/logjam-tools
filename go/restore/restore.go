package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/skaes/logjam-tools/go/util"
)

var opts struct {
	Verbose         bool   `short:"v" long:"verbose" description:"Be verbose."`
	Quiet           bool   `short:"q" long:"quiet" description:"Only log errors and warnings."`
	Dryrun          bool   `short:"n" long:"dryrun" description:"Don't perform the backup, list what would happen."`
	BackupDir       string `short:"b" long:"backup-dir" default:"." description:"Directory where to store backups."`
	DatabaseURL     string `short:"d" long:"database" default:"mongodb://localhost:27017" description:"Mongo DB host:port to restore to."`
	ExcludeRequests bool   `short:"x" long:"exclude-requests" description:"Don't restore request backups."`
	ToDate          string `short:"t" long:"to-date" description:"End date of backup period. Defaults to yesterday."`
	FromDate        string `short:"f" long:"from-date" description:"Start date of backup period. Defaults to zero time."`
	Match           string `short:"m" long:"match" default:"*" description:"Restrict restore to files matching the given file glob. Ignored when an explicit list of archives is given. Defaults to all files."`
}

var (
	rc                = int(0)
	verbose           = false
	dryrun            = false
	toDate            time.Time
	fromDate          time.Time
	archivesToRestore []string
)

const DATEFORMAT = "2006-01-02"

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
	verbose = opts.Verbose
	dryrun = opts.Dryrun
	if opts.ToDate != "" {
		t, err := time.Parse(DATEFORMAT, opts.ToDate)
		if err != nil {
			logError("could not parse to-date: %s. Error: %s", opts.ToDate, err)
			os.Exit(1)
		}
		toDate = t
	} else {
		// yesterday at the beginning of the day
		toDate = time.Now().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	}
	if opts.FromDate != "" {
		t, err := time.Parse(DATEFORMAT, opts.FromDate)
		if err != nil {
			logError("could not parse from-date: %s. Error: %s", opts.FromDate, err)
			os.Exit(1)
		}
		fromDate = t.Truncate(24 * time.Hour)
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

func restoreArchive(archive string) {
	_, err := os.Stat(archive)
	if err != nil {
		logInfo("could not access archive %s: %s", archive, err)
		return
	}
	cmd := exec.Command("mongorestore", "--drop", "--uri="+opts.DatabaseURL, "--archive="+archive, "--gzip")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logInfo("restoring archive %s", archive)
	if dryrun {
		return
	}
	err = cmd.Run()
	if err != nil {
		logError("restoring archive failed: %s", err)
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

func parseBackupDate(path string) (time.Time, error) {
	r := regexp.MustCompile(`(\d\d\d\d-\d\d-\d\d)`)
	matches := r.FindStringSubmatch(filepath.Base(path))
	if len(matches) == 2 {
		return time.Parse(DATEFORMAT, matches[1])
	}
	return time.Time{}, errors.New("missing date")
}

type archiveInfo struct {
	Date time.Time
	Path string
}

func extractArchiveInfo(a string) *archiveInfo {
	file := filepath.Base(a)
	date, err := parseBackupDate(file)
	if err != nil {
		logError("Ignoring archive '%s' because backup date could not be extracted: %s", err)
		return nil
	}
	return &archiveInfo{Date: date, Path: a}
}

func restoreFromBackups() {
	if len(archivesToRestore) == 0 {
		files, err := filepath.Glob(filepath.Join(opts.BackupDir, "*"))
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
	sort.Slice(archives, func(i int, j int) bool {
		younger := archives[i].Date.Before(archives[j].Date)
		sameDate := archives[i].Date == archives[j].Date
		return younger || (sameDate && strings.Compare(archives[i].Path, archives[j].Path) == -1)
	})
	for _, a := range archives {
		if util.Interrupted() {
			return
		}
		name, suffix := parseBackupFileName(a.Path)
		if name == "" {
			continue
		}
		if a.Date.Before(fromDate) || a.Date.After(toDate) {
			continue
		}
		if !opts.ExcludeRequests || suffix == ".archive" {
			restoreArchive(name + suffix)
		}
	}
}

func main() {
	initialize()
	logInfo("%s: starting restore in %s", os.Args[0], opts.BackupDir)
	util.InstallSignalHandler()
	restoreFromBackups()
	logInfo("%s: restore complete", os.Args[0])
	os.Exit(rc)
}
