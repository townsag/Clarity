package main

import (
	"fmt"
	"github.com/townsag/clarity/crdt"
)

func main() {
    crdt := crdt.NewDummyCRDT()

    for i, letter := range "This is my dummy sentence" {
        crdt.Insert(rune(letter), i)
    }
    fmt.Println("deleting from the sentence")
    crdt.Delete(8)
    fmt.Println("printing the crdt")
    fmt.Println(crdt)
}