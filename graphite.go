package graphite

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/rcrowley/go-metrics"
)

// Config provides a container with configuration parameters for
// the Graphite exporter
type Config struct {
	Addr          *net.TCPAddr     // Network address to connect to
	Registry      metrics.Registry // Registry to be exported
	FlushInterval time.Duration    // Flush interval
	DurationUnit  time.Duration    // Time conversion unit for durations
	Prefix        string           // Prefix to be prepended to metric names
	Percentiles   []float64        // Percentiles to export from timers and histograms
}

// Graphite is a blocking exporter function which reports metrics in r
// to a graphite server located at addr, flushing them every d duration
// and prepending metric names with prefix.
func Graphite(r metrics.Registry, d time.Duration, prefix string, addr *net.TCPAddr) {
	WithConfig(Config{
		Addr:          addr,
		Registry:      r,
		FlushInterval: d,
		DurationUnit:  time.Nanosecond,
		Prefix:        prefix,
		Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
	})
}

// WithConfig is a blocking exporter function just like Graphite,
// but it takes a GraphiteConfig instead.
func WithConfig(c Config) {
	for _ = range time.Tick(c.FlushInterval) {
		if err := graphite(&c); nil != err {
			log.Println(err)
		}
	}
}

// Once performs a single submission to Graphite, returning a
// non-nil error on failed connections. This can be used in a loop
// similar to GraphiteWithConfig for custom error handling.
func Once(c Config) error {
	return graphite(&c)
}

func graphite(c *Config) error {
	now := time.Now().Unix()
	du := float64(c.DurationUnit)
	flushSeconds := float64(c.FlushInterval) / float64(time.Second)
	conn, err := net.DialTCP("tcp", nil, c.Addr)
	if nil != err {
		return err
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	c.Registry.Each(func(name string, i interface{}) {
		name, tags := splitNameAndTags(name)
		switch metric := i.(type) {
		case metrics.Counter:
			count := metric.Count()
			fmt.Fprintf(w, "%s.%s.count%s %d %d\n", c.Prefix, name, tags, count, now)
			fmt.Fprintf(w, "%s.%s.count_ps%s %.2f %d\n", c.Prefix, name, tags, float64(count)/flushSeconds, now)
		case metrics.Gauge:
			fmt.Fprintf(w, "%s.%s.value%s %d %d\n", c.Prefix, name, tags, metric.Value(), now)
		case metrics.GaugeFloat64:
			fmt.Fprintf(w, "%s.%s.value%s %f %d\n", c.Prefix, name, tags, metric.Value(), now)
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles(c.Percentiles)
			fmt.Fprintf(w, "%s.%s.count%s %d %d\n", c.Prefix, name, tags, h.Count(), now)
			fmt.Fprintf(w, "%s.%s.min%s %d %d\n", c.Prefix, name, tags, h.Min(), now)
			fmt.Fprintf(w, "%s.%s.max%s %d %d\n", c.Prefix, name, tags, h.Max(), now)
			fmt.Fprintf(w, "%s.%s.mean%s %.2f %d\n", c.Prefix, name, tags, h.Mean(), now)
			fmt.Fprintf(w, "%s.%s.std-dev%s %.2f %d\n", c.Prefix, name, tags, h.StdDev(), now)
			for psIdx, psKey := range c.Percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				fmt.Fprintf(w, "%s.%s.%s-percentile%s %.2f %d\n", c.Prefix, name, tags, key, ps[psIdx], now)
			}
		case metrics.Meter:
			m := metric.Snapshot()
			fmt.Fprintf(w, "%s.%s.count%s %d %d\n", c.Prefix, name, tags, m.Count(), now)
			fmt.Fprintf(w, "%s.%s.one-minute%s %.2f %d\n", c.Prefix, name, tags, m.Rate1(), now)
			fmt.Fprintf(w, "%s.%s.five-minute%s %.2f %d\n", c.Prefix, name, tags, m.Rate5(), now)
			fmt.Fprintf(w, "%s.%s.fifteen-minute%s %.2f %d\n", c.Prefix, name, tags, m.Rate15(), now)
			fmt.Fprintf(w, "%s.%s.mean%s %.2f %d\n", c.Prefix, name, tags, m.RateMean(), now)
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles(c.Percentiles)
			count := t.Count()
			fmt.Fprintf(w, "%s.%s.count%s %d %d\n", c.Prefix, name, tags, count, now)
			fmt.Fprintf(w, "%s.%s.count_ps%s %.2f %d\n", c.Prefix, name, tags, float64(count)/flushSeconds, now)
			fmt.Fprintf(w, "%s.%s.min%s %d %d\n", c.Prefix, name, tags, t.Min()/int64(du), now)
			fmt.Fprintf(w, "%s.%s.max%s %d %d\n", c.Prefix, name, tags, t.Max()/int64(du), now)
			fmt.Fprintf(w, "%s.%s.mean%s %.2f %d\n", c.Prefix, name, tags, t.Mean()/du, now)
			fmt.Fprintf(w, "%s.%s.std-dev%s %.2f %d\n", c.Prefix, name, tags, t.StdDev()/du, now)
			for psIdx, psKey := range c.Percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				fmt.Fprintf(w, "%s.%s.%s-percentile%s %.2f %d\n", c.Prefix, name, tags, key, ps[psIdx]/du, now)
			}
			fmt.Fprintf(w, "%s.%s.one-minute%s %.2f %d\n", c.Prefix, name, tags, t.Rate1(), now)
			fmt.Fprintf(w, "%s.%s.five-minute%s %.2f %d\n", c.Prefix, name, tags, t.Rate5(), now)
			fmt.Fprintf(w, "%s.%s.fifteen-minute%s %.2f %d\n", c.Prefix, name, tags, t.Rate15(), now)
			fmt.Fprintf(w, "%s.%s.mean-rate%s %.2f %d\n", c.Prefix, name, tags, t.RateMean(), now)
		default:
			log.Printf("unable to record metric of type %T\n", i)
		}
		w.Flush()
	})
	return nil
}

// the input string name may contain tags
// e.g given input string name="disk.used;datacenter=dc1;rack=a1;server=web01"
// will return ("disk.used", "datacenter=dc1;rack=1a;server=web01")
// name and tags are separated by semicolon ";"
// refer to https://graphite.readthedocs.io/en/latest/tags.html
func splitNameAndTags(name string) (string, string) {
	if strings.Contains(name, ";") {
		splitted := strings.SplitN(name, ";", 2)
		if len(splitted) == 2 {
			return splitted[0], ";" + splitted[1]
		} else {
			return name, ""
		}
	} else {
		return name, ""
	}
}
