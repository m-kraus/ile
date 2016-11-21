package main

// /vagrant/ile/ile > /tmp/log && thruk -a logcacheupdate --local /tmp/log && thruk -a logcacheauthupdate

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

// Configuration describes the logexporter configurations
type Configuration struct {
	Timeout string `json:"timeout"`
	Thrukdb struct {
		Dsn      string `json:"dsn"`
		Db       string `json:"db"`
		User     string `json:"user"`
		Password string `json:"password"`
	} `json:"thruk_db"`
	Icingadb []struct {
		Dsn      string `json:"dsn"`
		Db       string `json:"db"`
		User     string `json:"user"`
		Password string `json:"password"`
	} `json:"icinga_db"`
}

// Logentry is a single logentry
type Logentry struct {
	Timestamp int
	Message   string
}

// Logentries is a collection of logentries
type Logentries []Logentry

// LoadConfig handles loading and parsing of JSON configuration file
func LoadConfig(path string) Configuration {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal("Config File Missing: ", err)
	}

	var config Configuration
	err = json.Unmarshal(file, &config)
	if err != nil {
		log.Fatal("Config Parse Error: ", err)
	}

	return config
}

func main() {

	var (
		flagConfig       string
		query            string
		thrukstatustable string
		thruklastupdate  int
	)

	// Parse flag -c for config file
	flag.StringVar(&flagConfig, "c", "", "Absolute path of ile config file.")
	flag.Parse()

	// Set default if no config file is given
	if flagConfig == "" {
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		flagConfig = dir + "/ile_config.json"
	}

	// Load configuration
	config := LoadConfig(flagConfig)

	// Iintialize variables
	thrukstatustable = ""
	thruklastupdate = 0

	// Prepare local dsn to query thruk logcache db
	thrukDsn := config.Thrukdb.User + ":" + config.Thrukdb.Password + "@" + config.Thrukdb.Dsn + "/?timeout=" + config.Timeout + "&parseTime=true&readTimeout=60s&writeTimeout=60s"

	// DB connection pool
	db, err := sql.Open("mysql", thrukDsn)
	if err != nil {
		log.Println("Cannot initialize db connection pool for thruk db, skipping: ", err)
	}
	defer db.Close()

	// Test actual db connection
	err = db.Ping()
	if err != nil {
		log.Println("Cannot connect to thruk db, skipping:", err)
	}

	// Get thruk tablename
	query = "SELECT table_name FROM information_schema.tables status where table_schema='" + config.Thrukdb.Db + "' and table_name like '%_status%'"
	db.QueryRow(query).Scan(&thrukstatustable) // ignore err

	// We found a thrukstatustable, get thruklastupdate
	if thrukstatustable != "" {
		// Retrieve last status update
		query = "SELECT value FROM " + config.Thrukdb.Db + "." + thrukstatustable + " WHERE name = 'last_update'"
		db.QueryRow(query).Scan(&thruklastupdate) // ignore err

		// Select 5 minutes more than thruklastupdate - thruk handles duplicates anyway
		if thruklastupdate > 0 {
			thruklastupdate -= 300
		}
	}

	// Initialize Logentries struct
	var icingalog Logentries

	// Connect to each given peer
	for _, i := range config.Icingadb {

		// Construct icinga dsn
		icingaDsn := i.User + ":" + i.Password + "@" + i.Dsn + "/?timeout=" + config.Timeout + "&parseTime=true&readTimeout=60s&writeTimeout=60s"

		// DB connection pool
		db, err := sql.Open("mysql", icingaDsn)
		if err != nil {
			log.Println("Cannot initialize db connection pool, skipping: ", err)
			continue
		}
		defer db.Close()

		// Test actual db connection
		err = db.Ping()
		if err != nil {
			// TODO
			log.Println("Cannot connect to database, skipping:", err)
			continue
		}

		// Get icinga log entries
		query = "SELECT UNIX_TIMESTAMP(logentry_time) AS timestamp, logentry_data AS message FROM " + i.Db + ".icinga_logentries WHERE logentry_time > FROM_UNIXTIME('" + strconv.Itoa(thruklastupdate) + "')"
		rows, err := db.Query(query)
		if err != nil {
			log.Println("Error getting icinga logentries: ", err)
			continue
		}
		defer rows.Close()

		var r Logentry
		for rows.Next() {
			err = rows.Scan(&r.Timestamp, &r.Message)
			if err != nil {
				log.Println("Error parsing result rows: ", err)
			}
			icingalog = append(icingalog, r)
		}
		err = rows.Err()
		if err != nil {
			log.Println("Error in result row iteration: ", err)
		}

	}

	for _, l := range icingalog {
		fmt.Println("[" + strconv.Itoa(l.Timestamp) + "] " + l.Message)
	}
}
