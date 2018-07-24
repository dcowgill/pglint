package main

import "github.com/jackc/pgx/pgtype"

// Finds indexes that are exact duplicates of one another and groups them into
// sets. All but one index in each set is superfluous.
func findDuplicateIndexSets(db *DB) ([][]*Index, error) {
	indexes, err := db.allIndexes()
	if err != nil {
		return nil, err
	}
	var answer [][]*Index
	for len(indexes) > 0 {
		duplicates, rest := bisectIndexes(indexes, indexes[0].EquivalentTo)
		if len(duplicates) >= 2 {
			answer = append(answer, duplicates)
		}
		indexes = rest
	}
	return answer, nil
}

// Returns indexes whose statistics indicate they have been scanned at most
// cutoff times. Such indexes are possibly superfluous.
func findUnusedIndexes(db *DB, cutoff int) ([]*Index, error) {
	indexes, err := db.allIndexes()
	if err != nil {
		return nil, err
	}
	return filterIndexes(indexes, func(ind *Index) bool {
		return ind.NumScans() <= cutoff
	}), nil
}

// Returns a slice of index pairs where the first index in the pair is made
// redundant by the second index in the pair.
func findRedundantIndexPairs(db *DB) ([][2]*Index, error) {
	indexes, err := db.allIndexes()
	if err != nil {
		return nil, err
	}

	// Group the indexes by table so that small sets can be compared.
	indexesByTable := make(map[pgtype.OID][]*Index)
	for _, ind := range indexes {
		if !ind.IsPrimary() && !ind.IsUnique() {
			indexesByTable[ind.TableOID()] = append(indexesByTable[ind.TableOID()], ind)
		}
	}

	// For each unique pair of indexes within a table, test whether the
	// first is redundant w/r/t the second. If so, append to answer.
	var answer [][2]*Index
	for _, indexes := range indexesByTable {
		for _, ind1 := range indexes {
			for _, ind2 := range indexes {
				if ind1 != ind2 && isRedundantIndex(ind1, ind2) {
					answer = append(answer, [2]*Index{ind1, ind2})
					break // next index
				}
			}
		}
	}
	return answer, nil
}

// Reports whether ind1 is redundant w/r/t ind2, which means all of the
// following are true: ind1's attributes are a strict prefix of ind2's; they
// have identical predicates; they are either both unique or both non-unique.
func isRedundantIndex(ind1, ind2 *Index) bool {
	return ind1.IsUnique() == ind2.IsUnique() && ind1.Pred() == ind2.Pred() && prefixOf(ind1, ind2)
}

// Reports whether ind1's attributes are a strict prefix of ind2's attributes.
// For example, if ind1 were an index on "X, Y" and ind2 on "X, Y, Z", ind1
// would be a prefix of ind2 (but not if ind2 were also on "X, Y").
func prefixOf(ind1, ind2 *Index) bool {
	attrs1 := ind1.Attrs()
	attrs2 := ind2.Attrs()
	if len(attrs1) >= len(attrs2) {
		return false // a must have fewer attributes than b
	}
	for i, x := range attrs1 {
		y := attrs2[i]
		if x != y {
			return false
		}
	}
	return true
}

// bisectIndexes returns two slices: the first contains values in xs for which
// pred returns true; the second contains the other values. N.B. modifies xs in
// place; the two returned slices are subslices of xs.
func bisectIndexes(xs []*Index, pred func(*Index) bool) ([]*Index, []*Index) {
	n := len(xs)
	for i := 0; i < n; {
		if pred(xs[i]) {
			xs[i], xs[n-1] = xs[n-1], xs[i]
			n--
		} else {
			i++
		}
	}
	return xs[n:], xs[:n]
}

// filterIndexes returns the values in xs for which pred returns true.
func filterIndexes(xs []*Index, pred func(*Index) bool) []*Index {
	var answer []*Index
	for _, x := range xs {
		if pred(x) {
			answer = append(answer, x)
		}
	}
	return answer
}
