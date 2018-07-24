package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/pgtype"
)

// Syntactic convenience. Implemented by pgx.Rows/Row.
type scannable interface {
	Scan(dest ...interface{}) error
}

// Bytes represents a count of bytes. Its main use is for reporting data
// volumes in a human-friendly format.
type Bytes int64

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

// MiB reports b in mebiBytes.
func (b Bytes) MiB() float64 { return float64(b) / float64(MiB) }

// Human reports the number of bytes as a human-readable string.
func (b Bytes) Human() string {
	switch {
	case b >= TiB:
		return fmt.Sprintf("%.1f TiB", float64(b)/float64(TiB))
	case b >= GiB:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(GiB))
	case b >= MiB:
		return fmt.Sprintf("%.1f MiB", b.MiB())
	case b >= KiB:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(KiB))
	}
	return strconv.Itoa(int(b)) + " B"
}

// oidVector corresponds to the Postgres type "oidvector".
//
// Neither the binary nor the text representation of an oidvector
// appears to be documented, but the text representation seems
// straightforward: a sequence of integer strings, separated by
// whitespace.
//
type oidVector []pgtype.OID

// DecodeText is part of the TextDecoder interface.
func (vec *oidVector) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return nil // vector is empty
	}
	fields := strings.Fields(string(src))
	*vec = make(oidVector, len(fields))
	for i, s := range fields {
		// N.B. OID is an unsigned 32-bit int, so we decode its string
		// repr as a 64-bit signed int to prevent overflow.
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		(*vec)[i] = pgtype.OID(n)
	}
	return nil
}

// Reports whether two oidVectors contain the same values.
func (vec oidVector) equal(rhs oidVector) bool {
	if len(vec) != len(rhs) {
		return false
	}
	for i, x := range vec {
		if x != rhs[i] {
			return false
		}
	}
	return true
}

// int2Vector corresponds to the Postgres type "int2vector".
type int2Vector []int16

// DecodeText is part of the TextDecoder interface.
func (vec *int2Vector) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return nil // vector is empty
	}
	fields := strings.Fields(string(src))
	*vec = make(int2Vector, len(fields))
	for i, s := range fields {
		n, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		(*vec)[i] = int16(n)
	}
	return nil
}

// Reports whether two int2Vectors contain the same values.
func (vec int2Vector) equal(rhs int2Vector) bool {
	if len(vec) != len(rhs) {
		return false
	}
	for i, x := range vec {
		if x != rhs[i] {
			return false
		}
	}
	return true
}
