package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	synthetis "github.com/bbvtaev/synthetis"
)

// In main func we have nothing to initialize,
// but we should create test user that writes,
// reads and does stuff. That will be localed
// here, in main.

func main() {
	start := time.Now()

	sth, err := synthetis.Open("./data/mtx.bin")
	if err != nil {
		slog.Info("Error occured", "err", err)
		return
	}
	defer sth.Close()

	done := make(chan struct{})
	var wg sync.WaitGroup

	ticker := time.NewTicker(1 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := sth.Write("each_millisecond_metric",
					map[string]string{"user": "pooser"},
					float64(rand.Intn(34)),
				); err != nil {
					slog.Info("Error occured", "err", err)
					return
				}
			}
		}
	}()

	for i := 0.0; i < 100000; i++ {
		if err := sth.Write("some_mtx",
			map[string]string{"host": "a"},
			i, 52, float64(rand.Intn(52)),
		); err != nil {
			slog.Info("Error occured", "err", err)
			close(done)
			wg.Wait()
			return
		}
	}

	close(done)
	wg.Wait()

	res, err := sth.Query("each_millisecond_metric", map[string]string{"user": "pooser"}, 1)
	if err != nil {
		slog.Info("Error occured", "err", err)
		return
	}

	fmt.Println(res)
	fmt.Println(time.Since(start))
}
