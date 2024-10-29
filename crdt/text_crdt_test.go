package crdt

import (
	"testing"
	"fmt"
)

func repersentationToString(representation []interface{}) (string, error) {
	var runes []rune
	for _, elem := range representation {
		r, ok := elem.(rune)
		if !ok {
			return "", fmt.Errorf("got an element of the representation that was not a rune: %v, type %T", elem, elem) 
		}
		runes = append(runes, r)
	}
	return string(runes), nil
}

func TestInsertSimple(t *testing.T) {
	var want string = "This is my example sentance"
	var crdt *TextCRDT = NewTextCRDT("replica1")
	for index, char := range want {
		crdt.LocalInsert(int64(index), rune(char))
	}
	repr, err := repersentationToString(crdt.Representation())
	if err != nil {
		panic(err)
	}
	if repr != want {
		t.Errorf("representation <%s> is not the same as want <%s>", repr, want)
	}
}

func TestInsertBackwards(t *testing.T) {
	var want string = "This is my example sentance"
	var crdt *TextCRDT = NewTextCRDT("replica1")
	for sourceIndex := len(want) - 1; sourceIndex >= 0; sourceIndex-- {
		crdt.LocalInsert(0, rune(want[sourceIndex]))
	}
	repr, err := repersentationToString(crdt.Representation())
	if err != nil {
		panic(err)
	}
	if repr != want {
		t.Errorf("representation <%s> is not the same as want <%s>", repr, want)
	}
}