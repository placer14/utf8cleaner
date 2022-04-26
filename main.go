package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
)

func main() {
	bs := bufio.NewScanner(os.Stdin)
	for bs.Scan() {
		b := bs.Bytes()
		fmt.Println(string(bytes.ToValidUTF8(b, []byte("?"))))
	}
}
