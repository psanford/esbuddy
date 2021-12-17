package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	elastic "github.com/olivere/elastic/v7"
	naturaldate "github.com/tj/go-naturaldate"
)

var (
	url      = flag.String("url", "http://localhost:9200", "Elasticsearch URL")
	index    = flag.String("index", "", "Elasticsearch index name")
	typ      = flag.String("type", "", "Elasticsearch type name")
	size     = flag.Int("size", 10000, "Slice of documents to get per scroll")
	sniff    = flag.Bool("sniff", false, "Enable or disable sniffing")
	queryStr = flag.String("query", "", "Query string")
	since    = flag.String("since", "15m", "Start time of query")
	until    = flag.String("until", "0m", "End time of query")
	count    = flag.Bool("count", false, "Return count of results only")
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	if *url == "" {
		log.Fatal("missing url parameter")
	}
	if *index == "" {
		log.Fatal("missing index parameter")
	}
	if *size <= 0 {
		log.Fatal("size must be greater than zero")
	}

	if *queryStr == "" {
		log.Fatal("missing query str")
	}

	now := time.Now()

	startTime, err := parseDate(*since, now)
	if err != nil {
		log.Fatalf("Error parsing -since field: %s", err)
	}

	if startTime.After(now) {
		log.Printf("Warning, -since is after now: since=%s now=%s", startTime, now)
	}

	endTime, err := parseDate(*until, now)
	if err != nil {
		log.Fatalf("Error parsing -until field: %s", err)
	}

	if endTime.Before(startTime) {
		log.Fatalf("-since must be before -until, since=%s until=%s", startTime, endTime)
	}

	log.Printf("connect: %s start=%s end=%s", *url, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	client, err := elastic.NewClient(elastic.SetURL(*url), elastic.SetSniff(*sniff))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	query :=
		elastic.NewBoolQuery().Must(
			elastic.NewQueryStringQuery(*queryStr),
			elastic.NewRangeQuery("@timestamp").From(startTime).To(endTime),
		)

	if *count {
		svc := client.Count(*index).Query(query)
		resp, err := svc.Do(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Printf("hits: %d\n", resp)
		return
	}

	svc := client.Scroll(*index).Query(query).Size(*size)

	for {
		res, err := svc.Do(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		for _, searchHit := range res.Hits.Hits {
			fmt.Printf("%s\n", searchHit.Source)
		}
	}
}

func parseDate(s string, now time.Time) (time.Time, error) {

	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return time.Time{}, err
		}

		if days > 0 {
			days = -days
		}
		return now.Add(time.Duration(days) * 24 * time.Hour), nil
	}

	duration, err := time.ParseDuration(s)
	if err == nil {
		if duration > 0 {
			duration *= -1
		}
		return now.Add(duration), nil
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t, nil
	}

	return naturaldate.Parse(*since, now)
}
