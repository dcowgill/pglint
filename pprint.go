package main

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Formats the headers and rows as a table and writes the result to w. The
// result is also compatible with GitHub's markdown syntax for tables.
//
// The rows may contain values of any data type. If a value implements either
// fmt.Stringer or fmt.GoStringer, the interface is used to convert it to a
// string (fmt.Stringer is preferred). ints and float64s are formatted using the
// fmt "%d" and "%g" verbs, respectively; anything else uses the "%v" verb.
//
// rows need not be of equal length; short rows are padded with empty cells. If
// a row is longer than headers, however, its extra values are not printed.
//
// prefix is appended to the beginning of each line.
//
func pprintTable(w io.Writer, headers []string, rows [][]interface{}, prefix string) {
	// Convert the rows to [][]string.
	strRows := make([][]string, len(rows))
	for i, row := range rows {
		strs := make([]string, len(row))
		for j, cell := range row {
			var s string
			switch v := cell.(type) {
			case fmt.Stringer:
				s = v.String()
			case fmt.GoStringer:
				s = v.GoString()
			case string:
				s = v
			case int:
				s = strconv.Itoa(v)
			case float64:
				s = fmt.Sprintf("%g", v)
			default:
				s = fmt.Sprintf("%v", cell)
			}
			strs[j] = s
		}
		strRows[i] = strs
	}

	// Determine the widest string in each column.
	colWidths := make([]int, len(headers))
	for i, s := range headers {
		colWidths[i] = len(s)
	}
	for _, row := range strRows {
		for i := 0; i < len(row) && i < len(headers); i++ {
			if n := len(row[i]); n > colWidths[i] {
				colWidths[i] = n
			}
		}
	}

	// Create the string that separates the headers from the rows.
	var separator string
	for _, w := range colWidths {
		separator += "|" + strings.Repeat("-", w+2)
	}
	separator += "|"

	// Helper func to print contents of a cell in column n.
	formatCell := func(n int, value string) string {
		align := ""
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			align = "-" // left align non-numeric cells
		}
		format := fmt.Sprintf("| %%%s%ds ", align, colWidths[n])
		return fmt.Sprintf(format, value)
	}

	// Helper function to emit a single row.
	printRow := func(row []string) {
		fmt.Fprint(w, prefix)
		for i := 0; i < len(row) && i < len(headers); i++ {
			fmt.Fprint(w, formatCell(i, row[i]))
		}
		for i := len(row); i < len(headers); i++ {
			fmt.Fprint(w, formatCell(i, ""))
		}
		fmt.Fprintf(w, "|\n")
	}

	// Print to the writer.
	printRow(headers)
	fmt.Fprintln(w, prefix+separator)
	for _, row := range strRows {
		printRow(row)
	}
}

// Like pprintTable but returns the output as a string.
func pprintTableString(headers []string, rows [][]interface{}, prefix string) string {
	buf := new(bytes.Buffer)
	pprintTable(buf, headers, rows, prefix)
	return buf.String()
}
