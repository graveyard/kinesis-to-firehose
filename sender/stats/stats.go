package stats

import (
	"time"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log = logger.New("kinesis-to-firehose-log-search")

type datum struct {
	app   string
	level string
}

var queue = make(chan datum, 2)

func init() {
	droppedLogsByApp := map[string]int{}
	droppedLogsByLevel := map[string]int{}
	total := 0
	tick := time.Tick(time.Minute)
	go func() {
		for {
			select {
			case d := <-queue:
				droppedLogsByApp[d.app]++
				droppedLogsByLevel[d.level]++
				total++
			case <-tick:
				tmp := logger.M{
					"total_dropped": total,
					"app_count":     len(droppedLogsByApp),
					"apps":          droppedLogsByApp,
					"level":         droppedLogsByLevel,
				}
				log.TraceD("drop-stats", tmp)

				droppedLogsByApp = map[string]int{}
				droppedLogsByLevel = map[string]int{}
				total = 0
			}
		}
	}()
}

// LogDropped keeps a running count of the dropped logs grouped by app and log-level
func LogDropped(log map[string]interface{}) {
	app, ok := log["container_app"].(string)
	if !ok || app == "" {
		app = "_UNKNOWN_"
	}
	level, ok := log["level"].(string)
	if !ok || level == "" {
		level = "debug"
	}

	queue <- datum{app, level}
}
