package utils

import (
	"encoding/json"
	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

func ToJson(i interface{}) string {
	marshal, _ := json.Marshal(i)
	return string(marshal)
}

func GetBsonM(i interface{}) *bson.M {
	var ret bson.M
	_ = mapstructure.Decode(i, &ret)
	return &ret
}

func GetTimePtr(t time.Time) *time.Time {
	cstSh, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		cstSh = time.FixedZone("CST", 8*3600)
	}
	in := t.In(cstSh)
	return &in
}
