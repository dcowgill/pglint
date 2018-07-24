// pgvet generates a report of problems found in a Postgres database. Its
// heuristics depend on live data and statistics, so its output should therefore
// only be trusted when run on a production instance.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jackc/pgx"
)

// TODO: identify smallint/integer sequences where overflow is imminent.

func main() {
	// Command-line flags.
	var (
		connInfo     = flag.String("conninfo", "host=localhost port=5432", "Postgres conninfo string or URI")
		namespace    = flag.String("namespace", "public", "schema to analyze")
		verbose      = flag.Bool("verbose", false, "enable verbose logging")
		unusedCutoff = flag.Int("unusedcutoff", 10, "treat indexes with this many scans or fewer as unused")
		minIndexSize = flag.Int("minindexsize", 1, "min. size (MiB) for unused index to be included in report")
		minIndexRows = flag.Int("minindexrows", 10, "min. rows for unused index to be included in report")
	)
	flag.Parse()

	// Parse the connection string.
	connConf, err := pgx.ParseConnectionString(*connInfo)
	if err != nil {
		fatalf("invalid Postgres conninfo string %q: %s", *connInfo, err)
	}

	// Set the logging level for the underlying database driver.
	connConf.LogLevel = pgx.LogLevelWarn
	if *verbose {
		connConf.LogLevel = pgx.LogLevelTrace
	}

	// For better errors, specify user and DB in lieu of implicit defaults.
	if connConf.User == "" {
		connConf.User = os.Getenv("USER")
	}
	if connConf.Database == "" {
		connConf.Database = connConf.User
	}

	// Open a connection to the database.
	conn, err := pgx.Connect(connConf)
	if err != nil {
		fatalf("failed to connect: %s", err)
	}

	// Fetch the info we need from the database and look for anomalies.
	db := newDB(conn, *namespace)
	allIndexes, err := db.allIndexes()
	if err != nil {
		fatalf("%+v", err)
	}
	duplicates, err := findDuplicateIndexSets(db)
	if err != nil {
		fatalf("%+v", err)
	}
	unused, err := findUnusedIndexes(db, *unusedCutoff)
	if err != nil {
		fatalf("%+v", err)
	}
	redundants, err := findRedundantIndexPairs(db)
	if err != nil {
		fatalf("%+v", err)
	}

	// Generate and print a report.
	rp := &reportPrinter{
		ConnConfig:             connConf,
		AllIndexes:             allIndexes,
		DuplicateIndexSets:     duplicates,
		UnusedIndexes:          unused,
		RedundantIndexPairs:    redundants,
		UnusedIndexScansCutoff: *unusedCutoff,
		MinIndexSize:           Bytes(*minIndexSize * 1024 * 1024),
		MinIndexRowCount:       *minIndexRows,
	}
	if err := rp.generate(os.Stdout); err != nil {
		fatalf("%+v", err)
	}

	// Close the connection.
	if err := conn.Close(); err != nil {
		fatalf("error while closing connection: %+v", err)
	}
}

// fatalf prints the message to stderr, then aborts.
func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}
