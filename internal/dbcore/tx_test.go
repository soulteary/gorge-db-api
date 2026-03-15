package dbcore

import "testing"

func TestSavepointName(t *testing.T) {
	txm := &TxManager{}
	txm.depth = 1
	if name := txm.savepointName(); name != "Aphront_Savepoint_1" {
		t.Errorf("got %q, want Aphront_Savepoint_1", name)
	}
	txm.depth = 3
	if name := txm.savepointName(); name != "Aphront_Savepoint_3" {
		t.Errorf("got %q, want Aphront_Savepoint_3", name)
	}
}
