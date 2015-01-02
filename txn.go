package collie

import "github.com/bsm/collie/column"

// A collection transaction. Transactions are not thread-safe
// and must not be used across multiple goroutines.
type Txn struct {
	c     *Collection
	stash []Record
}

func newTxn(coll *Collection, stash int) *Txn {
	return &Txn{
		c:     coll,
		stash: make([]Record, 0, stash),
	}
}

// New initializes an empty row and stashes it for the next commit
func (t *Txn) New() *Row {
	row := newRow(len(t.c.columns), len(t.c.indices))
	t.stash = append(t.stash, row)
	return row
}

// Add stashes a record for the next commit
func (t *Txn) Add(rec Record) {
	t.stash = append(t.stash, rec)
}

// Commit commits the transaction
func (t *Txn) Commit() (offset int64, err error) {
	var cval Value
	var ivals []Value
	updates := make([]indexUpdate, 0, len(t.c.indices)*len(t.stash)*2)

	t.c.wmux.Lock()
	defer t.c.wmux.Unlock()

	current := t.c.Offset()
	offset = current
	for _, rec := range t.stash {
		for name, col := range t.c.columns {
			if cval, err = rec.ValueAt(name); err != nil {
				goto Rollback
			} else if err = col.Add(cval); err != nil {
				goto Rollback
			}
		}

		for name, idx := range t.c.indices {
			if ivals, err = rec.IValuesAt(name); err != nil {
				goto Rollback
			}
			for _, val := range ivals {
				if err = idx.Add(val, offset); err != nil {
					goto Rollback
				}
				updates = append(updates, indexUpdate{i: idx, v: val, o: offset})
			}
		}
		offset++
	}

	t.c.storeOffset(offset)
	return

Rollback:
	offset = current
	for _, col := range t.c.columns {
		col.Truncate(offset)
	}
	for i := len(updates) - 1; i >= 0; i-- {
		update := updates[i]
		update.i.Undo(update.v, update.o)
	}
	return
}

// Discard reset the stash
func (t *Txn) Discard() {
	t.stash = t.stash[:0]
}

type indexUpdate struct {
	i column.Index
	o int64
	v Value
}
