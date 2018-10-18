package es

import (
	"context"

	"github.com/olivere/elastic"
)

type esHandler struct {
	esClient *elastic.Client
}

func (h *esHandler) CreateIndex(index, mapping string) error {
	_, err := h.esClient.CreateIndex(index).Body(mapping).Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (h *esHandler) AddDoc(index, itemType string, doc interface{}) error {
	_, err := h.esClient.Index().
		Index(index).
		Type(itemType).
		BodyJson(doc).
		Refresh("wait_for").
		Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (h *esHandler) Search(index string, termQuery *elastic.TermQuery, sortField string, size int) (*elastic.SearchResult, error) {
	searchResult, err := h.esClient.Search().
		Index(index).            // search in index "tweets"
		Query(termQuery).        // specify the query
		Sort(sortField, true).   // sort by "user" field, ascending
		From(0).Size(size).      // take documents 0-size
		Pretty(true).            // pretty print request and response JSON
		Do(context.Background()) // execute
	if err != nil {
		return nil, err
	}

	return searchResult, nil
}

func (h *esHandler) DeleteIndex(index ...string) error {
	_, err := h.esClient.DeleteIndex(index...).Do(context.Background())
	return err
}
