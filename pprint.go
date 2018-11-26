package main

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Formats the headers and rows as a table using Github-compatible markdown,
// printing to the supplied writer. The rows may contain values of any data
// type. If a value implements either fmt.Stringer or fmt.GoStringer, it is used
// to convert it to a string; fmt.Stringer is preferred. ints and float64s are
// formatted using the "%d" and "%f" fmt verbs; anything else uses "%v". The
// rows need not be of equal length: short rows are padded with empty cells;
// extra cells (that is, in rows longer than "headers") are ignored. Finally,
// "prefix" is appended to the beginning of each line in the output.
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
				s = fmtInt(v)
			case float64:
				s = fmtFloat(v)
			default:
				s = fmt.Sprintf("%v", cell)
			}
			strs[j] = s
		}
		strRows[i] = strs
	}

	// Columns that exclusively contain values of type int or float64 should be
	// right-aligned; everything else gets left-aligned.
	alignLeft := make([]bool, len(headers))
nextCol:
	for i := range headers {
		for _, row := range rows {
			switch row[i].(type) {
			case int, float64:
			default:
				alignLeft[i] = true
				continue nextCol
			}
		}
	}

	// FIXME: min(3, ...)
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

	// Create the string that separates the headers from the rows. It also
	// specifies which columns should be left- and right-aligned.
	var sepb strings.Builder
	for i, w := range colWidths {
		sepb.WriteString("| ")
		if alignLeft[i] {
			sepb.WriteByte(':')
		}
		for j := 0; j < w-1; j++ {
			sepb.WriteByte('-')
		}
		if !alignLeft[i] {
			sepb.WriteByte(':')
		}
		sepb.WriteByte(' ')
	}
	sepb.WriteByte('|')
	separator := sepb.String()

	// Create the format strings for each column. This could be combined with
	// the loop that builds the separator string, but it's clearer this way.
	formats := make([]string, len(headers))
	for i, width := range colWidths {
		align := ""
		if alignLeft[i] {
			align = "-"
		}
		formats[i] = fmt.Sprintf("| %%%s%ds ", align, width)
	}

	// Helper func: print a row. Works for the headers, too.
	printRow := func(row []string) {
		fmt.Fprint(w, prefix)
		for i := 0; i < len(row) && i < len(headers); i++ {
			fmt.Fprint(w, fmt.Sprintf(formats[i], row[i]))
		}
		for i := len(row); i < len(headers); i++ {
			fmt.Fprint(w, fmt.Sprintf(formats[i], ""))
		}
		fmt.Fprint(w, "|\n")
	}

	// Print the table the writer.
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

// Formats an integer in a locale-specific way, if possible.
func fmtInt(n int) string {
	if msgPrinter != nil {
		return msgPrinter.Sprintf("%d", n)
	}
	return strconv.Itoa(n)
}

// Formats a float in a locale-specific way, if possible.
func fmtFloat(n float64) string {
	if msgPrinter != nil {
		return msgPrinter.Sprintf("%f", n)
	}
	return fmt.Sprintf("%f", n)
}

// The global locale-specific printer.
var msgPrinter *message.Printer

func setLanguage(lang language.Tag) {
	msgPrinter = message.NewPrinter(lang)
}
