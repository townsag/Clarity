package crdt

import (
	"fmt"
)

type DummyCRDT struct {
	text []rune
}

func NewDummyCRDT() *DummyCRDT {
	return &DummyCRDT{
		text: []rune{},
	}
}

func (d *DummyCRDT) Insert(key rune, index int) error {
	// increace the capacity of the text slice if necessary
	fmt.Printf("Inserting %v at %v\n", key, index)
	fmt.Printf("len(d.text)= %v, cap(d.text)= %v\n", len(d.text), cap(d.text))
	if len(d.text) == cap(d.text) {
		fmt.Println("inside if statement")
		new_slice := make([]rune, len(d.text) + 1, 2 * len(d.text) + 1)
		copy(new_slice, d.text)		// dest to src
		d.text = new_slice
	} else {
		d.text = append(d.text, 0)
	}
	fmt.Println()
	// copy over all the keys >= index
	copy(d.text[index+1:], d.text[index:])
	// insert the key at index
	d.text[index] = key
	return nil
}

func (d *DummyCRDT) Delete(index int) error {
	// will this 
	d.text = append(d.text[:index], d.text[index+1:]...)
	return nil
}

func (d *DummyCRDT) Traverse(index int) ([]rune, error) {
	if len(d.text) <= index {
		return []rune{}, nil
	} else {
		following_elements, err := d.Traverse(index + 1)
		if err != nil {
			panic(err)
		}
		return append([]rune{d.text[index]}, following_elements...), nil
	}
}

func (d *DummyCRDT) String() string {
	contents, err := d.Traverse(0)
	if err != nil {
		panic(err)
	}
	return string(contents)
}