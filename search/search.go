package search

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/psanford/esbuddy/config"
	"github.com/psanford/esbuddy/date"
	"github.com/spf13/cobra"
)

var (
	urlFlag   string
	indexFlag string

	size      int
	sniff     bool
	since     string
	until     string
	limitFlag int
	ordered   bool
)

func Command() *cobra.Command {
	cmd := cobra.Command{
		Use:   "search <query>",
		Short: "Search ES",
		Run:   searchAction,
	}

	cmd.Flags().StringVarP(&urlFlag, "url", "", "http://localhost:9200", "Elasticsearch URL")
	cmd.Flags().StringVarP(&indexFlag, "index", "", "", "Index pattern")
	cmd.Flags().StringVarP(&since, "since", "", "15m", "Start time of query")
	cmd.Flags().StringVarP(&until, "until", "", "0m", "End time of query")

	cmd.Flags().IntVarP(&size, "size", "", 10000, "Slice of documents to get per scroll")
	cmd.Flags().IntVarP(&limitFlag, "limit", "", 0, "Max limit of results to return (0 is unlimited)")

	cmd.Flags().BoolVarP(&ordered, "ordered", "", true, "Query ordered by time desc")
	cmd.Flags().BoolVarP(&sniff, "sniff", "", false, "Enable sniffing")

	return &cmd

}

func searchAction(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Fatalf("Usage: search <query>")
	}

	queryStr := strings.Join(args, " ")

	conf := config.LoadConfig()
	if indexFlag == "" {
		indexFlag = conf.DefaultIndex
	}

	if urlFlag == "" {
		urlFlag = conf.URL
	}

	if size <= 0 {
		log.Fatal("size must be greater than zero")
	}

	if urlFlag == "" {
		log.Fatal("missing url parameter")
	}
	if indexFlag == "" {
		log.Fatal("missing index parameter")
	}

	now := time.Now()

	startTime, err := date.Parse(since, now)
	if err != nil {
		log.Fatalf("Error parsing -since field: %s", err)
	}

	if startTime.After(now) {
		log.Printf("Warning, -since is after now: since=%s now=%s", startTime, now)
	}

	endTime, err := date.Parse(until, now)
	if err != nil {
		log.Fatalf("Error parsing -until field: %s", err)
	}

	if endTime.Before(startTime) {
		log.Fatalf("-since must be before -until, since=%s until=%s", startTime, endTime)
	}

	log.Printf("connect: %s start=%s end=%s", urlFlag, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	client, err := elastic.NewClient(elastic.SetURL(urlFlag), elastic.SetSniff(sniff))
	if err != nil {
		log.Fatal(err)
	}

	if limitFlag > 0 && limitFlag < size {
		size = limitFlag
	}

	ctx := context.Background()

	query :=
		elastic.NewBoolQuery().Must(
			elastic.NewQueryStringQuery(queryStr),
			elastic.NewRangeQuery("@timestamp").From(startTime).To(endTime),
		)

	svc := client.Scroll(indexFlag).Query(query).Size(size)
	if ordered {
		sorter := elastic.NewFieldSort("@timestamp").Desc()
		svc = svc.SortBy(sorter)
	}

	defer svc.Clear(ctx)

	var count int
OUTER:
	for {
		res, err := svc.Do(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		for _, searchHit := range res.Hits.Hits {
			count++
			fmt.Printf("%s\n", searchHit.Source)
			if limitFlag > 0 && count >= limitFlag {
				break OUTER
			}
		}
	}
}
