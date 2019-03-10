package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/jessevdk/go-flags"
)

var opts struct {
	Verbose         bool   `short:"v" long:"verbose" description:"be verbose"`
	Dryrun          bool   `short:"n" long:"dryrun" description:"don't perform the backup, list what would happen"`
	BackupDir       string `short:"b" long:"backup-dir" default:"." description:"Directory where to store backups"`
	DatabaseURL     string `short:"d" long:"database" default:"mongodb://localhost:27017" description:"Mongo DB host:port to restore to"`
	ExcludeRequests bool   `short:"x" long:"exclude-requests" description:"don't restore request backups"`
}

var (
	rc          = int(0)
	verbose     = false
	dryrun      = false
	interrupted bool
)

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
	if _, err := os.Stat(opts.BackupDir); os.IsNotExist(err) {
		logError("backup directory does not exist")
		os.Exit(1)
	}
	verbose = opts.Verbose
	dryrun = opts.Dryrun
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

func restoreArchive(db, suffix string) {
	archive := filepath.Join(opts.BackupDir, db+suffix)
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

func parseBackupFileName(name string) (string, string) {
	for _, s := range []string{".archive", ".requests"} {
		f := strings.TrimSuffix(name, s)
		if f != name {
			return f, s
		}
	}
	return "", ""
}

func restoreFromBackups() {
	files, err := ioutil.ReadDir(opts.BackupDir)
	if err != nil {
		logError("could not read backup dir: %s", err)
		return
	}
	for _, f := range files {
		if interrupted {
			return
		}
		name, suffix := parseBackupFileName(f.Name())
		if name != "" && (!opts.ExcludeRequests || suffix == ".archive") {
			restoreArchive(name, suffix)
		}
	}
}

func main() {
	initialize()
	logInfo("%s: starting restore", os.Args[0])
	installSignalHandler()
	restoreFromBackups()
	logInfo("%s: restore complete", os.Args[0])
	os.Exit(rc)
}
