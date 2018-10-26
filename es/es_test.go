package es

import (
	"github.com/olivere/elastic"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zhiruchen/es-example/esclient"
)

var (
	userindex  = "user"
	msgIndex   = "msg"
	eventIndex = "event"
	swipeIndex = "swipe"

	userMapping = `{
		"settings":{
			"number_of_shards":1,
			"number_of_replicas":0
		},
		"mappings":{
			"_doc":{
				"properties":{
					"name":{
						"type":"keyword"
					},
					"age":{
						"type":"integer"
					},
					"job":{
						"type":"keyword"
					},
					"address":{
						"type":"keyword"
					}
				}
			}
		}
	}`
	msgMapping = `{
		"settings":{
			"number_of_shards":1,
			"number_of_replicas":0
		},
		"mappings":{
			"_doc":{
				"properties":{
					"content":{
						"type":"text"
					},
					"create_time":{
						"type":"date"
					},
					"author":{
						"type":"object"
					}
				}
			}
		}
	}`
	eventMapping = `{
		"settings":{
			"number_of_shards":1,
			"number_of_replicas":0
		},
		"mappings":{
			"_doc":{
				"properties":{
					"event_name":{
						"type":"keyword"
					},
					"location":{
						"type":"geo_point"
					},
					"create_time":{
						"type":"date"
					}
				}
			}
		}
	}`

	swipeMapping = `
	{
		"settings":{
			"number_of_shards":1,
			"number_of_replicas":0
		},
		"mappings":{
			"_doc":{
				"properties":{
					"user_id": {
						"type":"keyword"
					},
					"event_name":{
						"type":"keyword"
					},
					"location":{
						"type":"geo_point"
					},
					"create_time":{
						"type":"date"
					}
				}
			}
		}
	}
`
)

type user struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Job     string `json:"job"`
	Address string `json:"address"`
}

type msg struct {
	Content    string    `json:"content"`
	CreateTime time.Time `json:"create_time"`
	Author     *user     `json:"author"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type event struct {
	EventName  string            `json:"event_name"`
	Location   *elastic.GeoPoint `json:"location"`
	CreateTime time.Time         `json:"create_time"`
}

type swipe struct {
	UserId     string            `json:"user_id"`
	EventName  string            `json:"event_name"`
	Location   *elastic.GeoPoint `json:"location"`
	CreateTime time.Time         `json:"create_time"`
}

func TestCreateIndex(t *testing.T) {
	cases := []struct {
		index   string
		mapping string
	}{
		{
			index:   userindex,
			mapping: userMapping,
		},
		{
			index:   msgIndex,
			mapping: msgMapping,
		},
		{
			index:   eventIndex,
			mapping: eventMapping,
		},
	}

	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}
	//defer func() {
	//	handler.DeleteIndex(cases[0].index, cases[1].index, cases[2].index)
	//}()

	for _, c := range cases {
		err := handler.CreateIndex(c.index, c.mapping)
		assert.Nil(t, err)
	}
}

func TestAddDoc(t *testing.T) {
	cases := []struct {
		index string
		doc   interface{}
	}{
		{
			index: userindex,
			doc: &user{
				Name:    "Bob",
				Age:     26,
				Job:     "Software Engineer",
				Address: "Beijing",
			},
		},
		{
			index: msgIndex,
			doc: &msg{
				Content:    "msg content",
				CreateTime: time.Now(),
				Author: &user{
					Name:    "Alice",
					Age:     26,
					Job:     "QA",
					Address: "Beijing",
				},
			},
		},
		{
			index: eventIndex,
			doc: &event{
				EventName:  " test event ",
				Location:   elastic.GeoPointFromLatLon(48.137154, 11.576124), //&Location{Latitude: float64(33.6), Longitude: float64(120.1)},
				CreateTime: time.Now(),
			},
		},
	}
	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}

	for _, c := range cases {
		err := handler.AddDoc(c.index, "_doc", c.doc)
		assert.Nil(t, err)
		log.Println(c.index, c.doc, err)
	}
}

func TestCreateEventDoc(t *testing.T) {
	evnt := &event{
		EventName:  "test event",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now(),
	}

	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}
	err := handler.AddDoc(eventIndex, "_doc", evnt)
	assert.Nil(t, err)
}

func TestCreateSwipeMapping(t *testing.T) {
	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}
	err := handler.CreateIndex(swipeIndex, swipeMapping)
	assert.Nil(t, err)
}

func TestCreateSwipeDocs(t *testing.T) {
	swp := &swipe{
		UserId:     "111",
		EventName:  "test event1",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now().Add(time.Second * 10),
	}

	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}
	err := handler.AddDoc(swipeIndex, "_doc", swp)
	assert.Nil(t, err)

	swp = &swipe{
		UserId:     "222",
		EventName:  "test event1",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now().Add(time.Second * 20),
	}

	err = handler.AddDoc(swipeIndex, "_doc", swp)
	assert.Nil(t, err)

	swp = &swipe{
		UserId:     "222",
		EventName:  "test event2",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now().Add(time.Second * 50),
	}

	err = handler.AddDoc(swipeIndex, "_doc", swp)
	assert.Nil(t, err)

	swp = &swipe{
		UserId:     "111",
		EventName:  "test event2",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now().Add(time.Second * 60),
	}

	err = handler.AddDoc(swipeIndex, "_doc", swp)
	assert.Nil(t, err)

	swp = &swipe{
		UserId:     "222",
		EventName:  "test event2",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now().Add(time.Second * 90),
	}

	err = handler.AddDoc(swipeIndex, "_doc", swp)
	assert.Nil(t, err)

	swp = &swipe{
		UserId:     "111",
		EventName:  "test event1",
		Location:   elastic.GeoPointFromLatLon(float64(1.2931), float64(103.807)), //&Location{Latitude: float64(1.2931), Longitude: float64(103.807)},
		CreateTime: time.Now().Add(time.Second * 100),
	}

	err = handler.AddDoc(swipeIndex, "_doc", swp)
	assert.Nil(t, err)
}

func TestAggSearch(t *testing.T) {
	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}
	res, err := handler.AggSearch(&swipeQueryReq{
		index:     swipeIndex,
		eventName: "test event2",
		latitude:  float64(1.2931),
		longitude: float64(103.807),
		radius:    float64(2),
	})

	assert.Nil(t, err)

	//for _, m := range res.Each(reflect.TypeOf(&swipe{})) {
	//	v, ok := m.(*swipe)
	//	if ok {
	//		log.Println(v.UserId, v.EventName, v.CreateTime, v.Location)
	//	}
	//}
	log.Println(res)
}

func TestEsHandler_SearchSortByCreateTime(t *testing.T) {
	esClient := esclient.NewESClient()
	handler := &esHandler{esClient: esClient}
	res, err := handler.SearchSortByCreateTime("msg", 10)
	assert.Nil(t, err)

	for _, m := range res.Each(reflect.TypeOf(&msg{})) {
		v, ok := m.(*msg)
		if ok {
			log.Println(v.Author, v.Content, v.CreateTime)
		}
	}
}

func TestSearch(t *testing.T) {

}
