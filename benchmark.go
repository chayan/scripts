package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"os"
	"sync"
	"time"
)

const sortedSetExpiryDuration = 3 * 24 * time.Hour

func main() {

	host := flag.String("h", "", "host")
	port := flag.String("p", "", "port")
	password := flag.String("a", "", "password")

	pool := flag.Int("c", 100, "pool-size. default 100")
	durationSec := flag.Int64("t", 60, "duration in seconds")

	flag.Parse()

	addr := *host + ":" + *port
	fmt.Print("connecting to ", addr)
	fmt.Println(" with pool size: ", *pool)
	fmt.Println(*password)

	rc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        []string{addr},
		Password:     *password,
		DialTimeout:  1000 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		PoolSize:     *pool,
	})

	ids := getIds()
	c := 0

	zDurs := make([]int64, 0)
	exDurs := make([]int64, 0)

	var mu sync.Mutex

	dur := time.Duration(*durationSec) * time.Second
	fmt.Println("running test for ", dur)
	timer := time.NewTimer(dur)
	var wg sync.WaitGroup
	for {
		select {
		case <-timer.C:
			fmt.Println("calls made: ", len(zDurs))
			var total int64
			for _, d := range zDurs {
				total += d
			}

			avg := total / int64(len(zDurs))
			fmt.Println("avg: ", avg)
			wg.Wait()
			fmt.Println("exiting")
			os.Exit(0)
		default:
			idx := c % 10000
			c++
			id := ids[idx]
			score := time.Now().Unix() * 1000
			val := NewRunnerSupplyHours(id, score).String()
			key := "SUPPLY_HOURS|" + id

			wg.Add(1)
			go func() {
				defer wg.Done()
				st1 := time.Now()
				err := rc.ZAdd(key, redis.Z{
					Score:  float64(score),
					Member: val,
				}).Err()

				if err != nil {
					//fmt.Println("error in Zadd", err)
				}

				st2 := time.Now()
				err = rc.Expire(key, sortedSetExpiryDuration).Err()

				if err != nil {
					//fmt.Println("error in Expire", err)
				}

				mu.Lock()
				zDurs = append(zDurs, time.Since(st1).Milliseconds())
				exDurs = append(exDurs, time.Since(st2).Milliseconds())
				mu.Unlock()
			}()

			time.Sleep(500 * time.Microsecond)
		}
	}
}

func getIds() []string {
	ids := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		ids[i] = uuid.New().String()
	}

	return ids
}

type RunnerSupplyHours struct {
	RunnerId    string  `json:"runnerid"`
	Meta        Meta    `json:"meta"`
	RunnerState string  `json:"runnerstate"`
	AreaId      string  `json:"areaid"`
	Lat         float64 `json:"lat"`
	Lng         float64 `json:"lng"`
	Ts          int64   `json:"ts"`
	Source      string  `json:"source"`
}

func NewRunnerSupplyHours(runnerId string, ts int64) *RunnerSupplyHours {
	areas := []string{runnerId, runnerId}
	return &RunnerSupplyHours{
		RunnerId: runnerId,
		Meta: Meta{
			CohortAreas: areas,
		},
		RunnerState: "FREE",
		AreaId:      runnerId,
		Lat:         77.132,
		Lng:         12.333,
		Ts:          ts,
		Source:      "espresso",
	}
}

type Meta struct {
	CohortAreas []string `json:"cohortAreas"`
}

func (r *RunnerSupplyHours) String() (runnerStr string) {
	bytes, _ := json.Marshal(r)
	runnerStr = string(bytes)
	return
}
