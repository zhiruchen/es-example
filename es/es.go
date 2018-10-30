package es

import (
	"context"
	"strconv"

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

func (h *esHandler) SearchSortByCreateTime(index string, size int) (*elastic.SearchResult, error) {
	searchResult, err := h.esClient.Search().
		Index(index).
		Sort("create_time", false).
		From(0).Size(size).
		Pretty(true).
		Do(context.Background())

	return searchResult, err
}

type swipeQueryReq struct {
	index, eventName            string
	latitude, longitude, radius float64
}

func (h *esHandler) AggSearch(q *swipeQueryReq) ([]string, error) {
	distance := elastic.NewGeoDistanceQuery("location")
	distance.Distance(strconv.Itoa(int(q.radius)) + "m")
	distance.Point(q.latitude, q.longitude)
	querys := []elastic.Query{
		elastic.NewTermQuery("event_name", q.eventName),
		distance,
	}
	boolQuery := elastic.NewBoolQuery().Filter(querys...)

	maxAgg := elastic.NewMaxAggregation().Field("create_time")
	termsAgg := elastic.NewTermsAggregation().Field("user_id").OrderByAggregation("neartime", false)
	termsAgg = termsAgg.SubAggregation("neartime", maxAgg)

	searchResult, err := h.esClient.Search().
		Index(q.index).
		Query(boolQuery).
		Aggregation("user_ids", termsAgg).
		From(0).Size(1000).
		Do(context.Background())

	// return searchResult, err

	//results, err := elasticsearch.DefaultClient.Search(&elasticsearch.ComplexQuery{
	//	Context: elasticsearch.QueryContext{
	//		Index:    "swipe",
	//		ItemType: "_doc",
	//	},
	//	Query:       elastic.NewFunctionScoreQuery().Query(boolQuery),
	//	Size:        int64(q.size),
	//	AggsName:    "merchant_ids",
	//	Aggregation: termsAgg,
	//},
	//	elasticsearch.Timeout("500ms"),
	//)
	//if err != nil {
	//	return nil, err
	//}

	rawRes := searchResult.Aggregations
	userIds, found := rawRes.Terms("user_ids")
	if !found {
		return nil, err
	}

	var uids []string
	for _, b := range userIds.Buckets {
		uid, ok := b.Key.(string)
		if !ok {
			continue
		}

		uids = append(uids, uid)
	}

	return uids, nil
}

func (h *esHandler) ScriptSearch(index string) (*elastic.SearchResult, error) {
	scriptFilter := elastic.NewScript(`doc['tags'].values.length > 0 && doc['tags'].values[0] != ""`).
		Lang("painless")
	baseFilter := []elastic.Query{
		elastic.NewScriptQuery(scriptFilter),
	}

	boolQuery := elastic.NewBoolQuery().Should(elastic.NewBoolQuery().Filter(baseFilter...))
	searchResult, err := h.esClient.Search().
		Index(index).
		Query(boolQuery).
		From(0).Size(1000).
		Do(context.Background())
	return searchResult, err
}

func (h *esHandler) DeleteIndex(index ...string) error {
	_, err := h.esClient.DeleteIndex(index...).Do(context.Background())
	return err
}
