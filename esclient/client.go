package esclient

import (
	"log"

	"github.com/olivere/elastic"
)

const (
	url = "http://localhost:9200"
)

func NewESClient() *elastic.Client {
	client, err := elastic.NewClient(elastic.SetURL(url), elastic.SetSniff(true))
	if err != nil {
		log.Fatal(err)
	}

	return client
}