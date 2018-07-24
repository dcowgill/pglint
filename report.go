package main

import (
	"fmt"
	"html/template"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx"
)

type reportPrinter struct {
	ConnConfig             pgx.ConnConfig
	AllIndexes             []*Index
	DuplicateIndexSets     [][]*Index
	UnusedIndexes          []*Index
	RedundantIndexPairs    [][2]*Index
	UnusedIndexScansCutoff int
	MinIndexSize           Bytes
	MinIndexRowCount       int

	relevantUnusedIndexes []*Index // cache
}

func (rp *reportPrinter) generate(w io.Writer) error {
	return tmpl(w, markdownReport, rp)
}

func (rp *reportPrinter) Now() string {
	return time.Now().Format(time.RFC1123)
}

func (rp *reportPrinter) NumDuplicateIndexSets() int { return len(rp.DuplicateIndexSets) }
func (rp *reportPrinter) FormatDuplicateIndexSets() string {
	if rp.NumDuplicateIndexSets() == 0 {
		return ""
	}
	var b strings.Builder
	sortIndexSetsByName(rp.DuplicateIndexSets)
	sep := ""
	for _, indexes := range rp.DuplicateIndexSets {
		_, _ = b.WriteString(sep)
		_, _ = b.WriteString(indexesTable(indexes))
		sep = "\n\n"
	}
	return b.String()
}

func sortIndexSetsByName(sets [][]*Index) {
	// Sort the indexs within each set by name.
	for _, indexes := range sets {
		sort.Sort(indexesByName(indexes))
	}
	// Then sort the sets by table name.
	sort.Slice(sets, func(i, j int) bool {
		ind1, ind2 := sets[i][0], sets[j][0]
		if ind1.TableName() == ind2.TableName() {
			return ind1.Name() < ind2.Name() // tie-breaker
		}
		return ind1.TableName() == ind2.TableName()
	})
}

func (rp *reportPrinter) NumUnusedIndexes() int { return len(rp.getRelevantUnusedIndexes()) }
func (rp *reportPrinter) FormatUnusedIndexes() string {
	if rp.NumUnusedIndexes() == 0 {
		return ""
	}
	return indexesTable(rp.getRelevantUnusedIndexes())
}

func (rp *reportPrinter) getRelevantUnusedIndexes() []*Index {
	if rp.relevantUnusedIndexes == nil {
		indexes := filterIndexes(rp.UnusedIndexes, func(ind *Index) bool {
			switch {
			case ind.Kind() == uniqueIndex:
				return false
			case ind.Size() < rp.MinIndexSize:
				return false
			case ind.NumRows() < rp.MinIndexRowCount:
				return false
			default:
				return true
			}
		})
		// Put non-PK indexes first, then sort by decreasing size.
		sort.Slice(indexes, func(i, j int) bool {
			if indexes[i].IsPrimary() != indexes[j].IsPrimary() {
				return indexes[j].IsPrimary()
			}
			return indexes[i].Size() > indexes[j].Size()
		})
		rp.relevantUnusedIndexes = indexes
	}
	return rp.relevantUnusedIndexes
}

func (rp *reportPrinter) NumRedundantIndexPairs() int { return len(rp.RedundantIndexPairs) }
func (rp *reportPrinter) FormatRedundantIndexPairs() string {
	if rp.NumRedundantIndexPairs() == 0 {
		return ""
	}
	sortIndexPairsBySize(rp.RedundantIndexPairs)
	rows := make([][]interface{}, len(rp.RedundantIndexPairs))
	for i, pair := range rp.RedundantIndexPairs {
		ind1, ind2 := pair[0], pair[1]
		rows[i] = []interface{}{
			ind1.QualifiedTableName(),
			ind1.Name(),
			ind2.Name(),
			ind1.Kind(),
			int(ind1.Size().MiB()),
			ind1.NumRows(),
			ind1.NumScans(),
			strings.Join(ind1.Attrs(), ", "),
			strings.Join(ind2.Attrs(), ", "),
		}
	}
	headings := []string{"Table", "Index1", "Index2", "T", "Size (MiB)", "Rows", "Scans", "Attrs1", "Attrs2"}
	// return "```\n" + pprintTableString(headings, rows, "") + "\n```"
	return pprintTableString(headings, rows, "")
}

func sortIndexPairsBySize(a [][2]*Index) {
	sort.Slice(a, func(i, j int) bool { return a[i][0].Size() > a[j][0].Size() })
}

// tmpl executes the given template text on data, writing the result to w.
func tmpl(w io.Writer, text string, data interface{}) error {
	t := template.New("top")
	template.Must(t.Parse(text))
	ew := &errWriter{w: w}
	err := t.Execute(ew, data)
	if ew.err != nil {
		// I/O error. Ignore write on closed pipe.
		if !strings.Contains(ew.err.Error(), "pipe") {
			return fmt.Errorf("writing report template: %v", ew.err)
		}
	}
	return err
}

// An errWriter wraps a writer, recording whether a write error occurred.
type errWriter struct {
	w   io.Writer
	err error
}

func (w *errWriter) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	if err != nil {
		w.err = err
	}
	return n, err
}

func indexesTable(indexes []*Index) string {
	rows := make([][]interface{}, len(indexes))
	for i, index := range indexes {
		rows[i] = []interface{}{
			index.QualifiedTableName(),
			index.Name(),
			index.Kind(),
			int(index.Size().MiB()),
			index.NumRows(),
			index.NumScans(),
			strings.Join(index.Attrs(), ", "),
		}
	}
	headings := []string{"Table", "Index", "T", "Size (MiB)", "Rows", "Scans", "Attrs"}
	return pprintTableString(headings, rows, "")
}

const markdownReport = `# pgvet report for database "{{ .ConnConfig.Database }}"

Connection info:

* Host: {{ .ConnConfig.Host }}
* Port: {{ .ConnConfig.Port }}
* User: {{ .ConnConfig.User }}
* Database: {{ .ConnConfig.Database }}

## Duplicate Indexes

Sets of duplicate indexes found: {{ .NumDuplicateIndexSets }}

Indexes in this section share an exact definition with at least one other index.
It is therefore always safe to drop one of the two.

{{ .FormatDuplicateIndexSets }}

## Redundant Indexes

Pairs of redundant indexes found: {{ .NumRedundantIndexPairs }}

In the following table, "Index1" refers to the redundant index, and "Attrs1" its
columns/expressions. It is usually safe to drop an index that is a prefix of
another index, as the latter can satisfy the same query plans.

{{ .FormatRedundantIndexPairs }}

## Unused Indexes

Unused indexes found: {{ .NumUnusedIndexes }}

Criteria for inclusion in this report:

* Scanned at most {{ .UnusedIndexScansCutoff }} times.
* Size greater than or equal to {{ .MinIndexSize.Human }}.
* Contains at least {{ .MinIndexRowCount }} rows.
* Is either non-unique or is a primary key.

**Important:** this section of the report relies on usage statistics, and will
only contain meaningful results if pgvet was run against a production database.

Note: unique indexes are not included because they enforce a constraint and
cannot be dropped simply because they aren't used in query plans (when a unique
index prevents its constraint from being violated, it is not recorded as a
"scan"). Primary key indexes, however, _are_ included: a primary key that is
never scanned is often a sign of a design flaw.

{{ .FormatUnusedIndexes }}

*Generated at {{ .Now }}*
`
