package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	locker := redislock.New(rdb)

	// channel task list
	taskCount := 10
	tasks := make(chan string, 10)
	for i := 0; i < taskCount; i++ {
		tasks <- fmt.Sprintf("task-%d", i)
	}
	close(tasks)

	// worker pool
	var wg sync.WaitGroup
	workerCount := 20

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(i, tasks, locker, &wg, r)
	}

	wg.Wait()
	fmt.Println("All tasks done!")

}

func worker(id int, tasks <-chan string, locker *redislock.Client, wg *sync.WaitGroup, r *rand.Rand) {
	defer wg.Done()

	for task := range tasks {
		lockKey := fmt.Sprintf("lock:%s", task)

		// retry locking 3 times maximum
		success := false
		for retry := 0; retry < 3; retry++ {
			lock, err := locker.Obtain(ctx, lockKey, 5*time.Minute, nil)
			if err == redislock.ErrNotObtained {
				// log failure to acquire lock and retry
				log.Printf("[Worker %d] Failed lock %s, retry #%d\n", id, task, retry+1)
				time.Sleep(time.Duration(r.Intn(2000)+100) * time.Millisecond) // wait random 100-300ms
				continue
			} else if err != nil {
				// log error during lock obtainment
				log.Printf("[Worker %d] Error obtain lock: %v\n", id, err)
				break
			}

			// lock obtained successfully
			log.Printf("[Worker %d] ✅ Processing %s\n", id, task)
			time.Sleep(time.Duration(r.Intn(1000)+500) * time.Millisecond) // simulate work for 0.5s-1.5s
			lock.Release(ctx)
			success = true
			break
		}

		if !success {
			// if all retries fail, log failure and skip task
			log.Printf("[Worker %d] ❌ Failed lock %s\n", id, task)
		}
	}
}
