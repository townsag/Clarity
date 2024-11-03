package crdt

import (
	"testing"
	"fmt"
)

func representationToString(representation []interface{}) (string, error) {
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
	var want string = "This is my example sentence"
	var crdt *TextCRDT = NewTextCRDT("replica1")
	for index, char := range want {
		crdt.LocalInsert(int64(index), rune(char))
	}
	rep, err := representationToString(crdt.Representation())
	if err != nil {
		panic(err)
	}
	if rep != want {
		t.Errorf("representation <%s> is not the same as want <%s>", rep, want)
	}
}

func TestDeleteSimple(t *testing.T) {
	var want string = "This is my sentence"
	var crdt *TextCRDT = NewTextCRDT("replica1")
	for index, char := range "This is my example sentence" {
		crdt.LocalInsert(int64(index), rune(char))
	}
	for i := 0; i < len("example "); i++ {
		crdt.LocalDelete(11)
	}
	rep, err := representationToString(crdt.Representation())
	if err != nil {
		panic(err)
	}
	if rep != want {
		t.Errorf("representation <%s> is not the same as want <%s>", rep, want)
	}
}

func TestInsertBackwards(t *testing.T) {
	var want string = "This is my example sentence"
	var crdt *TextCRDT = NewTextCRDT("replica1")
	for sourceIndex := len(want) - 1; sourceIndex >= 0; sourceIndex-- {
		crdt.LocalInsert(0, rune(want[sourceIndex]))
	}
	rep, err := representationToString(crdt.Representation())
	if err != nil {
		panic(err)
	}
	if rep != want {
		t.Errorf("representation <%s> is not the same as want <%s>", rep, want)
	}
}

func TestInsertMultiUserForward(t *testing.T) {
	var want string = "Grocery List: apple, banana"
	var crdt1 *TextCRDT = NewTextCRDT("replica1")
	var crdt2 *TextCRDT = NewTextCRDT("replica2")

	// first user inserts "Grocery List: "
	var operations1 []Operation = make([]Operation, 0)
	for index, value := range "Grocery List: " {
		newOperation := crdt1.LocalInsert(int64(index), rune(value))
		operations1 = append(operations1, newOperation)
	}
	
	// apply changes made by first user to second users 
	for _, operation := range operations1 {
		crdt2.Apply(operation)
	}

	//second user inserts "apple, banana"
	var operations2 []Operation = make([]Operation, 0)
	for index, value := range "apple, banana" {
		newOperation := crdt2.LocalInsert(int64(index + len("Grocery List: ")), rune(value))
		operations2 = append(operations2, newOperation)
	}

	// apply changes made by the second user to the first user
	for _, operation := range operations2 {
		crdt1.Apply(operation)
	}

	// test that the representation of both CRDTs is correct and equivalent
	representation1, err := representationToString(crdt1.Representation())
    if err != nil {
        panic(err)
    }
    representation2, err := representationToString(crdt2.Representation())
    if err != nil {
        panic(err)
    }
    if representation1 != want {
        t.Errorf("representation of crdt1 does not match want\nrep1: %s\nwant: %s", representation1, want)
    }
    if representation2 != want {
        t.Errorf("representation of crdt2 does not match want\nrep2: %s\nwan: %s", representation2, want)
    }
}

func TestInsertDeleteForwardsMultiUser(t *testing.T) {
	var want string = "Grocery List: apple, banana"
	var crdt1 *TextCRDT = NewTextCRDT("replica1")
	var crdt2 *TextCRDT = NewTextCRDT("replica2")

	// first user inserts grocery list
	var operations1 []Operation = make([]Operation, 0)
	for index, char := range "Grocery List: " {
		operations1 = append(operations1, crdt1.LocalInsert(int64(index), rune(char)))
	}

	// second user is updated with first users inserts
	for _, operation := range operations1 {
		crdt2.Apply(operation)
	}

	// second user adds "apple, banana, grapefruit" to the list
	var operations2 []Operation = make([]Operation, 0)
	for index, char := range "apple, banana, grapefruit" {
		operations2 = append(operations2, crdt2.LocalInsert(int64(index + len("Grocery List: ")), rune(char)))
	}

	// first user is updated with second users inserts
	for _, operation := range operations2 {
		crdt1.Apply(operation)
	}

	// first user deletes ", grapefruit"
	operations1 = make([]Operation, 0)
	for range ", grapefruit" {
		newOperation := crdt1.LocalDelete(int64(len("Grocery List: apple, banana,") - 1))
		// the crdt string representation is zero indexed. We want to forward starting from the "," after
		// banana to the end of the string. The index of the comma is equal to the length of the substring 
		// minus one
		temp, _ := representationToString(crdt1.Representation())
		fmt.Printf("this is rep in delete loop: %v\n", temp)
		operations1 = append(operations1, newOperation)
	}
	
	// apply the deletes to the the second crdt
	for _, operation := range operations1 {
		crdt2.Apply(operation)
	}

	// verify that the crdt representations are correct and equivalent
	representation1, err := representationToString(crdt1.Representation())
    if err != nil {
        panic(err)
    }
    representation2, err := representationToString(crdt2.Representation())
    if err != nil {
        panic(err)
    }
    if representation1 != want {
        t.Errorf("representation of crdt1 does not match want\nrep1: %s\nwant: %s", representation1, want)
    }
    if representation2 != want {
        t.Errorf("representation of crdt2 does not match want\nrep2: %s\nwan: %s", representation2, want)
    }
}

func TestConcurrentInsert(t *testing.T) {
	
}

func TestConcurrentInsertReverse(t *testing.T) {

}