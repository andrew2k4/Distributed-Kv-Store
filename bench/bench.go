package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "distributed_kv_store/internal/pb"

	"google.golang.org/grpc"
)

const address = "localhost:50051"

func main() {
	mode := flag.String("mode", "throughput", "benchmark mode: throughput or latency")

	// latency mode flags
	numRequests := flag.Int("n", 100000, "latency mode: total requests (split across -concurrency clients)")
	warmup := flag.Int("warmup", 2000, "latency mode: requests discarded before measuring (cold conn/caches)")
	latencyConcurrency := flag.Int("concurrency", 1, "latency mode: number of parallel sequential clients (1 = pure baseline)")

	// throughput mode flags
	levels := flag.String("levels", "1,10,50,100,200,500", "throughput mode: comma-separated concurrency levels to sweep")
	duration := flag.Duration("duration", 5*time.Second, "throughput mode: how long to run at each concurrency level")

	flag.Parse()

	switch *mode {
	case "latency":
		runLatencyBench(*numRequests, *warmup, *latencyConcurrency)
	case "throughput":
		runThroughputBench(parseLevels(*levels), *duration)
	default:
		log.Fatalf("unknown mode: %s (expected throughput or latency)", *mode)
	}
}

func parseLevels(csv string) []int {
	parts := strings.Split(csv, ",")
	levels := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("invalid concurrency level %q: %v", p, err)
		}
		levels = append(levels, n)
	}
	return levels
}

func dial() (*grpc.ClientConn, error) {
	return grpc.Dial(address, grpc.WithInsecure())
}

// --- Latency protocol: the honest p99 ---
//
// Sends requests sequentially per client (1 client = pure baseline, a small
// pool approximates light realistic load) and times each op individually.
// The first `warmup` requests are discarded so cold connections/caches don't
// skew the percentiles.

type latencyResult struct {
	latencies []time.Duration
	errCount  int
}

func runLatencyBench(numRequests, warmup, concurrency int) {
	if concurrency < 1 {
		log.Fatalf("concurrency must be >= 1")
	}
	if warmup >= numRequests {
		log.Fatalf("warmup (%d) must be less than n (%d)", warmup, numRequests)
	}

	perWorker := numRequests / concurrency
	warmupPerWorker := warmup / concurrency

	var wg sync.WaitGroup
	resultsCh := make(chan latencyResult, concurrency)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := dial()
			if err != nil {
				log.Fatalf("worker %d: failed to connect: %v", workerID, err)
			}
			defer conn.Close()
			client := pb.NewKvStoreClient(conn)

			latencies := make([]time.Duration, 0, perWorker-warmupPerWorker)
			errCount := 0

			for i := 0; i < perWorker; i++ {
				start := time.Now()
				_, err := client.SetHandler(context.Background(), &pb.SetRequest{
					Key:   fmt.Sprintf("key-%d-%d", workerID, i),
					Value: "value",
				})
				elapsed := time.Since(start)

				if i < warmupPerWorker {
					continue
				}
				if err != nil {
					errCount++
					continue
				}
				latencies = append(latencies, elapsed)
			}

			resultsCh <- latencyResult{latencies, errCount}
		}(w)
	}

	wg.Wait()
	close(resultsCh)

	var all []time.Duration
	errCount := 0
	for r := range resultsCh {
		all = append(all, r.latencies...)
		errCount += r.errCount
	}

	fmt.Printf("Mode: latency (concurrency=%d)\n", concurrency)
	fmt.Printf("Requests: %d total, %d warm-up discarded, %d measured, %d failed\n",
		numRequests, warmup, len(all), errCount)
	reportLatency(all)
}

func reportLatency(latencies []time.Duration) {
	if len(latencies) == 0 {
		fmt.Println("no measured requests")
		return
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}

	fmt.Printf("Avg: %v\n", sum/time.Duration(len(latencies)))
	fmt.Printf("Min: %v\n", latencies[0])
	fmt.Printf("P50: %v\n", latencies[int(float64(len(latencies))*0.50)])
	fmt.Printf("P95: %v\n", latencies[int(float64(len(latencies))*0.95)])
	fmt.Printf("P99: %v\n", latencies[int(float64(len(latencies))*0.99)])
	fmt.Printf("Max: %v\n", latencies[len(latencies)-1])
}

// --- Throughput protocol: ops/sec ---
//
// Concurrency is the variable being swept. At each level, a persistent pool
// of workers hammers the server for a fixed duration (long enough that
// warm-up doesn't dominate) and we report successful_ops / elapsed.

func runThroughputBench(levels []int, duration time.Duration) {
	fmt.Printf("%-12s%-14s%-12s%-12s%s\n", "Concurrency", "Ops", "Duration", "Ops/sec", "Errors")
	for _, c := range levels {
		ops, errCount, elapsed := runThroughputLevel(c, duration)
		opsPerSec := float64(ops) / elapsed.Seconds()
		fmt.Printf("%-12d%-14d%-12s%-12.0f%d\n", c, ops, elapsed.Round(time.Millisecond), opsPerSec, errCount)
	}
}

func runThroughputLevel(concurrency int, duration time.Duration) (opsCount, errCount int64, elapsed time.Duration) {
	var wg sync.WaitGroup
	var readyWg sync.WaitGroup
	start := make(chan struct{})
	stop := make(chan struct{})

	wg.Add(concurrency)
	readyWg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		go func(workerID int) {
			defer wg.Done()

			conn, err := dial()
			if err != nil {
				log.Fatalf("worker %d: failed to connect: %v", workerID, err)
			}
			defer conn.Close()
			client := pb.NewKvStoreClient(conn)

			readyWg.Done()
			<-start

			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}

				_, err := client.SetHandler(context.Background(), &pb.SetRequest{
					Key:   fmt.Sprintf("key-%d-%d-%d", concurrency, workerID, i),
					Value: "value",
				})
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				atomic.AddInt64(&opsCount, 1)
			}
		}(w)
	}

	readyWg.Wait()
	startAll := time.Now()
	close(start)
	time.AfterFunc(duration, func() { close(stop) })
	wg.Wait()
	elapsed = time.Since(startAll)

	return opsCount, errCount, elapsed
}
