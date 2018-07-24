package main

import (
	"github.com/jackc/pgx/pgtype"
)

// Categorizes an index based on uniqueness.
type indexKind string

const (
	uniqueIndex    indexKind = "U"
	nonUniqueIndex indexKind = "N"
	primaryKey     indexKind = "P"
)

// Kind reports the indexKind of v.
func (v *Index) Kind() indexKind {
	switch {
	case v.IsPrimary():
		return primaryKey
	case v.IsUnique():
		return uniqueIndex
	}
	return nonUniqueIndex
}

// Index contains information about a PostgreSQL index.
type Index struct {
	oid              pgtype.OID // unique identifier of the index
	name             string     // name of the index
	namespaceOID     pgtype.OID // OID of the index's namespace
	namespace        string     // the index namespace
	tableOID         pgtype.OID // unique identifier of the index's table
	tableName        string     // name of the index's table
	numColumns       int        // count of columns in the index
	isUnique         bool       // if true, index is unique
	isPrimary        bool       // if true, index represents table PK; IsUnique also true
	isValid          bool       // if true, currently valid for queries
	isLive           bool       // if false, index is being dropped and should be ignored
	keys             int2Vector // ordered list of column positions (1..N); 0 = expr
	collations       oidVector  // for each column, indicates collation used in index
	classes          oidVector  // for each column, indicates pg_opclass
	options          oidVector  // for each column, contains flag bits
	exprs            string     // computed expressions, one for each 0 in Keys
	pred             string     // partial index predicate; null if not partial index
	definition       *string    // reconstructed CREATE INDEX statement
	numPages         int        // count of disk pages in index
	numRows          int        // approximate count of tuples in index
	numTablePages    int        // count of disk pages in index's table
	numTableRows     int        // approximate count of tuples in index's table
	numScans         int        // number of times index scanned (since statistics collected)
	numTuplesRead    int        // count of tuples read from index
	numTuplesFetched int        // count of tuples fetched from index
	size             Bytes      // total size of index on disk

	attrs []string
}

func (v *Index) OID() pgtype.OID          { return v.oid }
func (v *Index) Name() string             { return v.name }
func (v *Index) NamespaceOID() pgtype.OID { return v.namespaceOID }
func (v *Index) Namespace() string        { return v.namespace }
func (v *Index) TableOID() pgtype.OID     { return v.tableOID }
func (v *Index) TableName() string        { return v.tableName }
func (v *Index) NumColumns() int          { return v.numColumns }
func (v *Index) IsUnique() bool           { return v.isUnique }
func (v *Index) IsPrimary() bool          { return v.isPrimary }
func (v *Index) IsValid() bool            { return v.isValid }
func (v *Index) IsLive() bool             { return v.isLive }
func (v *Index) Keys() int2Vector         { return v.keys }
func (v *Index) Collations() oidVector    { return v.collations }
func (v *Index) Classes() oidVector       { return v.classes }
func (v *Index) Options() oidVector       { return v.options }
func (v *Index) Exprs() string            { return v.exprs }
func (v *Index) Pred() string             { return v.pred }
func (v *Index) Definition() string       { return strVal(v.definition) }
func (v *Index) NumPages() int            { return v.numPages }
func (v *Index) NumRows() int             { return v.numRows }
func (v *Index) NumTablePages() int       { return v.numTablePages }
func (v *Index) NumTableRows() int        { return v.numTableRows }
func (v *Index) NumScans() int            { return v.numScans }
func (v *Index) NumTuplesRead() int       { return v.numTuplesRead }
func (v *Index) NumTuplesFetched() int    { return v.numTuplesFetched }
func (v *Index) Size() Bytes              { return v.size }

// Attrs returns the indexed fields, which may be column names or expressions.
func (v *Index) Attrs() []string { return v.attrs }

// QualifiedTableName returns the table name prefixed by its namespace. If the
// namespace is "public", however, it is omitted for brevity.
func (v *Index) QualifiedTableName() string {
	if v.namespace == "public" {
		return v.tableName
	}
	return v.namespace + "." + v.tableName
}

// QualifiedName returns the index name prefixed by its namespace. If the
// namespace is "public", however, it is omitted for brevity.
func (v *Index) QualifiedName() string {
	if v.namespace == "public" {
		return v.tableName
	}
	return v.namespace + "." + v.name
}

// EquivalentTo reports whether v is a structurally equivalent index to u.
func (v *Index) EquivalentTo(u *Index) bool {
	if v.OID() == u.OID() {
		return true // special case shortcut when v is u
	}
	return (v.TableOID() == u.TableOID() &&
		v.IsUnique() == u.IsUnique() &&
		v.Keys().equal(u.Keys()) &&
		v.Collations().equal(u.Collations()) &&
		v.Classes().equal(u.Classes()) &&
		v.Options().equal(u.Options()) &&
		v.Exprs() == u.Exprs() &&
		v.Pred() == u.Pred())
}

// Sorts indexes lexicographically by name.
type indexesByName []*Index

func (a indexesByName) Len() int           { return len(a) }
func (a indexesByName) Less(i, j int) bool { return a[i].Name() > a[j].Name() }
func (a indexesByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// strVal returns an empty string is s is nil, *s otherwise.
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
