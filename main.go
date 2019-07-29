package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/darthguinea/golib/log"
	"github.com/darthguinea/golib/try"
	_ "github.com/lib/pq"
)

type connection struct {
	Username      string
	Password      string
	Host          string
	DBName        string
	Timeout       string
	Query         string
	Count         int
	Passed        int64
	Failed        int64
	DateStarted   time.Time
	DateFailed    time.Time
	DatePassed    time.Time
	IsFailed      bool
	OutageElapsed time.Duration
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func queryDB(db *sql.DB, query string) {
	start := time.Now()

	row, err := db.Query(query)
	checkErr(err)

	var v interface{}
	for row.Next() {
		row.Scan(v)
	}

	elapsed := time.Since(start)
	miliseconds := elapsed * time.Microsecond
	log.Debug("Time taken %v", miliseconds)

	time.Sleep(1 * time.Second)
}

func cachedConnection(Con *connection) {
	dbinfo := fmt.Sprintf("host=%s user=%s password=%s dbname=%s connect_timeout=%s sslmode=disable",
		Con.Host, Con.Username, Con.Password, Con.DBName, Con.Timeout)

	log.Info("Using connection details: [%v]", dbinfo)

	db, err := sql.Open("postgres", dbinfo)
	checkErr(err)
	defer db.Close()

	for {
		try.This(func() {
			queryDB(db, Con.Query)
			Con.Passed++
		}).Finally(func() {
		}).Catch(func(e try.E) {
			log.Warn("Cached connection FAILED!")
			Con.Failed++
			time.Sleep(1 * time.Second)
		})
	}
}

func newConnection(Con *connection) {
	if Con.Count > 0 {
		var wg sync.WaitGroup
		Con.DateStarted = time.Now()

		for {
			wg.Add(Con.Count)
			log.Debug("wg count: [%v]", Con.Count)
			for x := 0; x < Con.Count; x++ {
				go threadedConnection(Con, &wg)
			}
			wg.Wait()
			log.Debug("Waiting to be done()")
			log.Info("Passed: [%v] Failed: [%v], Started[%v] Time Failed: [%v] Time Passed [%v] Elapsed (%v) IsDown: [%v]",
				Con.Passed,
				Con.Failed,
				Con.DateStarted.Format("2006-01-02 15:04:05"),
				Con.DateFailed.Format("2006-01-02 15:04:05"),
				Con.DatePassed.Format("2006-01-02 15:04:05"),
				Con.OutageElapsed,
				Con.IsFailed,
			)
		}
	}
}

func threadedConnection(Con *connection, wg *sync.WaitGroup) {
	dbinfo := fmt.Sprintf("host=%s user=%s password=%s dbname=%s connect_timeout=%s sslmode=disable",
		Con.Host, Con.Username, Con.Password, Con.DBName, Con.Timeout)
	try.This(func() {
		db, err := sql.Open("postgres", dbinfo)
		checkErr(err)
		queryDB(db, Con.Query)
		Con.Passed++
		if Con.IsFailed {
			Con.IsFailed = false
			Con.DatePassed = time.Now()
			Con.OutageElapsed = Con.OutageElapsed + time.Since(Con.DateFailed)
		}
		db.Close()
	}).Finally(func() {
	}).Catch(func(e try.E) {
		if Con.IsFailed == false {
			log.Warn("Connections Failing!")
			Con.IsFailed = true
			Con.DateFailed = time.Now()
		}
		Con.Failed++
		time.Sleep(1 * time.Second)
	})
	log.Debug("wg done")
	wg.Done()
}

func cleanup(Con *connection) {
	log.Print("")
	log.Print("")
	log.Info("Final Results: (User exited the program)")
	log.Info("\tTest started:\t\t[%v]", Con.DateStarted.Format("2006-01-02 15:04:05"))
	log.Info("\tTo database:\t\t[%v/%v]", Con.Host, Con.DBName)
	log.Info("\tSuccessful connections: [%v]", Con.Passed)
	log.Info("\tFailed connections:\t[%v]", Con.Failed)
	log.Info("\tTotal Outage time:\t[%v]", Con.OutageElapsed)
}

func main() {
	var Con connection
	Con.IsFailed = false
	flagLogLevel := log.INFO

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanup(&Con)
		os.Exit(1)
	}()

	flag.StringVar(&Con.Host, "h", "127.0.0.1", "-h <host> Set the host IP")
	flag.StringVar(&Con.DBName, "d", "database_name", "-d <dbname> Set the database name")
	flag.StringVar(&Con.Username, "u", "myuser", "-u <user> Set the username")
	flag.StringVar(&Con.Password, "p", "somepass", "-p <pass> Set the password")
	flag.StringVar(&Con.Timeout, "t", "1", "-t <timeout> Set the timeout in seconds (default: 1)")
	flag.StringVar(&Con.Query, "q", "SELECT id FROM account;", "-q <query_to_run>")
	flag.IntVar(&Con.Count, "c", 0, "-c <count> Thread count for new connections")
	flag.IntVar(&flagLogLevel, "l", 0, "-l <log_level> Set the log level, default is info")
	flag.Parse()

	log.SetLevel(log.DEBUG)

	if Con.Count > 0 {
		go cachedConnection(&Con)
	} else {
		cachedConnection(&Con)
	}
	newConnection(&Con)
}
