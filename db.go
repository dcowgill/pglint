package main

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// DB exposes a high-level interface to the Postgres information schema.
type DB struct {
	conn      *pgx.Conn
	namespace string
	indexes   []*Index
}

// Creates a new DB for the given connection.
func newDB(conn *pgx.Conn, namespace string) *DB {
	return &DB{conn: conn, namespace: namespace}
}

// Returns all indexes in the DB. The result is cached, but every call returns a
// unique slice, so it is safe for the caller to modify.
func (db *DB) allIndexes() ([]*Index, error) {
	if db.indexes == nil {
		result, err := loadIndexes(db.conn, db.namespace)
		if err != nil {
			return nil, err
		}
		db.indexes = result
	}
	a := make([]*Index, len(db.indexes))
	copy(a, db.indexes)
	return a, nil
}

// Returns all valid indexes in the database; q.v. DB.allIndexes.
func loadIndexes(conn *pgx.Conn, namespace string) ([]*Index, error) {
	// Fetch the basic index data.
	rows, err := conn.Query(sqlSelectIndexInfo, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var indexes []*Index
	for rows.Next() {
		var index Index
		if err := scanIndex(rows, &index); err != nil {
			return nil, err
		}
		indexes = append(indexes, &index)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// For each index, set its column attributes. Each attribute must either be
	// a column reference or an expression.
	//
	// We do so here for efficiency's sake; we can retrieve all of the column
	// names in a single query round trip. (The index expressions are stored
	// with the rest of the index metadata.)

	tableCols, err := loadIndexTableColumns(conn)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		attrs := make([]string, len(index.keys))
		exprs := splitExprs(index.Exprs())
		for i, key := range index.keys {
			switch {
			case key == 0:
				if len(exprs) == 0 {
					return nil, fmt.Errorf("index %q (%d): no expression for key #%d (value:0)",
						index.Name(), index.OID(), i)
				}
				attrs[i] = exprs[0]
				exprs = exprs[1:]
			case key > 0:
				if name, ok := tableCols[index.tableOID].lookup(int(key)); ok {
					attrs[i] = name
				} else {
					return nil, fmt.Errorf("index %q (%d): no column ref found for key %d (%d)",
						index.Name(), index.OID(), i, key)
				}
			}
		}
		index.attrs = attrs
	}

	// All done.
	return indexes, nil
}

// N.B. ignores non-live and invalid indexes.
const sqlSelectIndexInfo = `
select c.oid,
       c.relname,
       c.relnamespace,
       ns.nspname,
       i.indrelid,
       t.relname,
       i.indnatts,
       i.indisunique,
       i.indisprimary,
       i.indisvalid,
       i.indislive,
       i.indkey,
       i.indcollation,
       i.indclass,
       i.indoption,
       coalesce(pg_get_expr(i.indexprs, i.indrelid), ''),
       coalesce(pg_get_expr(i.indpred, i.indrelid), ''),
       t.relpages,
       t.reltuples::bigint,
       c.relpages,
       c.reltuples::bigint,
       coalesce(s.idx_scan, 0),
       coalesce(s.idx_tup_read, 0),
       coalesce(s.idx_tup_fetch, 0),
       (select pg_relation_size(c.oid)),
       (select indexdef
          from pg_indexes
         where schemaname = ns.nspname
           and tablename = t.relname
           and indexname = c.relname)
  from pg_index i
  join pg_class c on c.oid = i.indexrelid
  join pg_class t on t.oid = i.indrelid
  join pg_namespace ns on ns.oid = c.relnamespace
  left outer join pg_stat_user_indexes s on s.indexrelid = i.indexrelid
 where i.indislive is true and i.indisvalid is true
   and ns.nspname = $1`

func scanIndex(sc scannable, v *Index) error {
	return sc.Scan(
		&v.oid,              // pg_class.oid
		&v.name,             // pg_class.relname
		&v.namespaceOID,     // pg_class.relnamespace
		&v.namespace,        // pg_namespace.nspname
		&v.tableOID,         // pg_index.indrelid
		&v.tableName,        // pg_class[2].relname (table)
		&v.numColumns,       // pg_index.indnatts
		&v.isUnique,         // pg_index.indisunique
		&v.isPrimary,        // pg_index.indisprimary
		&v.isValid,          // pg_index.indisvalid
		&v.isLive,           // pg_index.indislive
		&v.keys,             // pg_index.indkey
		&v.collations,       // pg_index.indcollation
		&v.classes,          // pg_index.indclass
		&v.options,          // pg_index.indoption
		&v.exprs,            // pg_get_expr(pg_index.indexprs)
		&v.pred,             // pg_get_expr(pg_index.indpred)
		&v.numTablePages,    // pg_class[2].relpages (table)
		&v.numTableRows,     // pg_class[2].reltuples (table)
		&v.numPages,         // pg_class.relpages
		&v.numRows,          // pg_class.reltuples
		&v.numScans,         // pg_stat_user_indexes.idx_scan
		&v.numTuplesRead,    // pg_stat_user_indexes.idx_tup_read
		&v.numTuplesFetched, // pg_stat_user_indexes.idx_tup_fetch
		&v.size,             // pg_relation_size(pg_class.oid)
		&v.definition,       // pg_indexes.indexdef
	)
}

// Reads per-table column information from the connection and organizes it as a
// mapping from table OID to column list; q.v. type tableCols.
func loadIndexTableColumns(conn *pgx.Conn) (map[pgtype.OID]*tableCols, error) {
	rows, err := conn.Query(sqlSelectIndexTableColumnNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tc := make(map[pgtype.OID]*tableCols)
	for rows.Next() {
		var (
			id   pgtype.OID // table OID
			name string     // column name
			key  int        // column offset
		)
		if err := rows.Scan(&id, &name, &key); err != nil {
			return nil, err
		}
		tc[id] = tc[id].add(name, key)
	}
	return tc, nil
}

// Selects column attributes for all tables having at least one valid index.
const sqlSelectIndexTableColumnNames = `
select c.oid, a.attname, a.attnum
  from pg_class c
  join pg_attribute a on a.attrelid = c.oid
 where c.oid in (select indrelid from pg_index where indislive is true and indisvalid is true)
   and a.attnum >= 1`

type (
	// Associates column number and name.
	colNameKey struct {
		name string
		key  int // 1-based
	}

	// Stores the column names/offsets for a table.
	tableCols struct {
		columns []colNameKey
	}
)

// Adds a column to the table. Works if t if nil: allocates on demand and
// returns the new struct value.
func (t *tableCols) add(name string, key int) *tableCols {
	if t == nil {
		t = new(tableCols)
	}
	t.columns = append(t.columns, colNameKey{name, key})
	return t
}

// Reports the column name associated with the key.
func (t *tableCols) lookup(key int) (name string, found bool) {
	if t != nil {
		for _, c := range t.columns {
			if c.key == key {
				return c.name, true
			}
		}
	}
	return "", false
}

// Naively parses the output of pg_get_expr().
// E.g. "f(x, y), z, a + b" -> ["f(x, y)", "z", "a + b"]
func splitExprs(input string) []string {
	var (
		exprs []string // the result
		curr  []rune   // current sub-expression
		nest  = 0      // current parens nesting
	)
	for _, c := range input {
		switch c {
		case '(':
			nest++
		case ')':
			nest--
		case ' ':
			if len(curr) == 0 {
				continue
			}
		case ',':
			if nest == 0 {
				exprs = append(exprs, string(curr))
				curr = curr[:0]
				continue // skip
			}
		}
		curr = append(curr, c)
	}
	if len(curr) > 0 {
		exprs = append(exprs, string(curr))
	}
	return exprs
}
