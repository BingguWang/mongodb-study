package main

import (
	"context"
	"fmt"
	"github.com/BingguWang/mongodb-study/dao"
	"github.com/BingguWang/mongodb-study/datasource"
	"github.com/BingguWang/mongodb-study/utils"
	"go.mongodb.org/mongo-driver/bson"
)

func init() {
	datasource.InitMongo()
}
func main() {
	ctx := context.Background()
	client := datasource.GetMongoClient()
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
	/*databases, err := client.ListDatabases(ctx, bson.D{{}})
	if err != nil {
		panic(err)
	}
	fmt.Println("show databases:\n", utils.ToJson(databases))*/
	// use test, 不存在会创建
	db := client.Database("bing")
	fmt.Println(utils.ToJson(db))

	//cla := &model.Class{ClassId: 4, ClassName: "CS-4", ClassDesc: &model.ClassDesc{TeacherCount: 12, StuCount: 20}, Years: 13, Brief: "ewtrwefs  dsadsa wqD SDSdDWQ WQD "}
	//m := utils.GetBsonM(cla)
	//fmt.Println(m)
	//dao.AddClass(ctx, db, m)

	// bson.D内的条件是AND关系
	//class := dao.FindClassByFilter(ctx, db, &bson.D{{"ClassId", 3}})
	class := dao.FindClassByFilter(ctx, db, &bson.D{})
	fmt.Println("查询结果:", utils.ToJson(class))

	//_, _ = dao.UpdateClass(ctx, db, &bson.D{{"ClassName", "MBA-4"}}, &bson.M{"$set": bson.M{"ClassName": "MBA三班"}})
	dao.Aggregate(ctx, db)
	//dao.CreateIndexes(ctx, db)

	dao.ListIndex(ctx, db)

	// 全文检索
	dao.FindCommentByFullTextIndex(ctx, db)

}
