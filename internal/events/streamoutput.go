package events

import (
	"bufio"
	"io"
)

type LogLine struct {
	Line string
	Err  error
}

func StreamOutput(r io.Reader) <-chan LogLine {
	ch := make(chan LogLine, 1000)

	go func() {
		defer close(ch)

		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			ch <- LogLine{
				Line: scanner.Text(),
			}
		}

		if err := scanner.Err(); err != nil {
			ch <- LogLine{Err: err}
		}
	}()

	return ch
}
