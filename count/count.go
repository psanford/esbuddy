package count

import (
	"context"
	"fmt"
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

	sniff bool
	since string
	until string
)

func Command() *cobra.Command {
	cmd := cobra.Command{
		Use:   "count <query>",
		Short: "Count Only Query",
		Run:   searchAction,
	}

	cmd.Flags().StringVarP(&urlFlag, "url", "", "http://localhost:9200", "Elasticsearch URL")
	cmd.Flags().StringVarP(&indexFlag, "index", "", "", "Index pattern")
	cmd.Flags().StringVarP(&since, "since", "", "15m", "Start time of query")
	cmd.Flags().StringVarP(&until, "until", "", "0m", "End time of query")

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

	ctx := context.Background()

	query :=
		elastic.NewBoolQuery().Must(
			elastic.NewQueryStringQuery(queryStr),
			elastic.NewRangeQuery("@timestamp").From(startTime).To(endTime),
		)

	svc := client.Count(indexFlag).Query(query)
	resp, err := svc.Do(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("hits: %d\n", resp)
	return
}
