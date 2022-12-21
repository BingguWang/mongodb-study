package dao

import (
	"context"
	"fmt"
	"github.com/BingguWang/mongodb-study/model"
	"github.com/BingguWang/mongodb-study/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

// 新增班级
func AddClassWithTransaction(ctx context.Context, client *mongo.Client, db *mongo.Database, document interface{}) (interface{}, error) {
	session, err := client.StartSession()
	if err != nil {
		fmt.Printf("start session failed : %s\n", err.Error())
		return nil, err
	}
	defer session.EndSession(ctx)

	sessionContext := mongo.NewSessionContext(ctx, session)
	if err = session.StartTransaction(); err != nil {
		log.Printf("start transaction failed : %s\n", err.Error())
		return nil, err
	}

	// db.Collection不存在就会创建collection
	insertOneResult, err := db.Collection("class").InsertOne(sessionContext, document)
	if err != nil {
		log.Printf("insert failed : %s\n", err.Error())
		_ = session.AbortTransaction(context.Background())
		return nil, err
	}
	if err = session.CommitTransaction(context.Background()); err != nil {
		return nil, err
	}
	return insertOneResult, nil
}
func AddClass(ctx context.Context, db *mongo.Database, document interface{}) (interface{}, error) {
	// db.Collection不存在就会创建collection
	insertOneResult, err := db.Collection("class").InsertOne(ctx, document)
	if err != nil {
		log.Printf("insert failed : %s\n", err.Error())
		return nil, err
	}
	return insertOneResult, nil
}

// 查询class
func FindClassByFilter(ctx context.Context, db *mongo.Database, filter interface{}) []*model.Class {
	var ret []*model.Class
	//findCursor, err := db.Collection("class").Find(ctx, bson.D{}) // 查询所有

	// 按照name排序并跳过第一个, 且只需返回name、level字段
	findOneOpts := options.Find().
		SetSort(bson.D{{"classid", -1}}). // -1是降序
		//SetProjection(bson.D{{"ClassName", 1}}). // 要返回的字段
		SetLimit(50) // limit

	// db.Collection不存在就会创建collection
	findCursor, err := db.Collection("class").Find(ctx, filter, findOneOpts)
	if err != nil {
		panic(err)
	}
	defer findCursor.Close(ctx)

	for findCursor.Next(context.TODO()) {
		// 创建一个值，将单个文档解码为该值
		var elem model.Class
		err := findCursor.Decode(&elem)
		if err != nil {
			fmt.Println(err.Error()+" id:", findCursor.ID())
			continue
		}
		if elem.Id != nil {
			fmt.Println(utils.GetTimePtr(elem.Id.Timestamp()))
		}
		ret = append(ret, &elem)
	}
	return ret
}

// distinct只能对某个字段进行去重，然后只返回此字段
func DistinctClass(ctx context.Context, db *mongo.Database, filter interface{}) (interface{}, error) {
	distinctOpts := options.Distinct().SetMaxTime(2 * time.Second)
	// 返回所有不同的人名
	distinctValues, err := db.Collection("class").Distinct(ctx, "ClassId", filter, distinctOpts)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	fmt.Println(utils.ToJson(distinctValues))
	return distinctValues, nil
}

// 删除班级
func DeleteClass(ctx context.Context, db *mongo.Database, filter interface{}) (int64, error) {
	deleteOneOpts := options.Delete().SetCollation(&options.Collation{
		CaseLevel: false, // 忽略大小写
	})
	// filter是&bson.D{}时，会删除全部
	// 没有匹配的不会报错
	deleteResult, err := db.Collection("class").DeleteMany(ctx, filter, deleteOneOpts)
	if err != nil {
		// 不使用mongo的ctx，防止mongo的ctx超时时，也能成功abort
		return 0, err
	}
	fmt.Println("deletet count:", deleteResult.DeletedCount)
	return deleteResult.DeletedCount, nil
}

func DeleteClassWithTansaction(ctx context.Context, client *mongo.Client, db *mongo.Database, filter interface{}) (int64, error) {
	// 事务需要开启session
	sess, err := client.StartSession()
	if err != nil {
		log.Printf("start session failed : %s", err.Error())
		return 0, err
	}
	defer sess.EndSession(ctx)
	// session context, as an arg while calling api
	sessionCtx := mongo.NewSessionContext(ctx, sess)

	// session开启transaction
	if err = sess.StartTransaction(); err != nil {
		panic(err)
	}

	// DeleteOne
	deleteOneOpts := options.Delete().SetCollation(&options.Collation{
		CaseLevel: false, // 忽略大小写
	})
	// filter是&bson.D{}时，会删除全部
	// 没有匹配的不会报错
	deleteResult, err := db.Collection("class").DeleteMany(sessionCtx, filter, deleteOneOpts)
	if err != nil {
		// 不使用mongo的ctx，防止mongo的ctx超时时，也能成功abort
		_ = sess.AbortTransaction(context.Background())
		return 0, err
	}
	if err = sess.CommitTransaction(context.Background()); err != nil {
		return 0, err
	}
	fmt.Println("deletet count:", deleteResult.DeletedCount)
	return deleteResult.DeletedCount, nil
}

// 返回的只是个估计值
func CountClass(ctx context.Context, db *mongo.Database, filter interface{}) (int64, error) {
	// CountDocuments
	count, err := db.Collection("class").CountDocuments(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("count:", count)
	return count, nil
}

// 修改班级
func UpdateClass(ctx context.Context, client *mongo.Client, db *mongo.Database, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	// 事务需要开启session
	sess, err := client.StartSession()
	if err != nil {
		log.Printf("start session failed : %s", err.Error())
		return nil, err
	}
	defer sess.EndSession(ctx)
	// session context, as an arg while calling api
	sessionCtx := mongo.NewSessionContext(ctx, sess)

	// session开启transaction
	if err = sess.StartTransaction(); err != nil {
		log.Printf("start transaction failed : %s", err.Error())
		return nil, err
	}

	opts := options.Update().SetUpsert(true) // 开启upsert
	updateManyResult, err := db.Collection("class").UpdateMany(sessionCtx, filter, update, opts)
	if err != nil {
		_ = sess.AbortTransaction(context.Background())
		return nil, err
	}
	fmt.Printf(
		"matched: %d  modified: %d  upserted: %d  upsertedID: %v\n",
		updateManyResult.MatchedCount,  // filter匹配到的document数
		updateManyResult.ModifiedCount, // 更新的document数
		updateManyResult.UpsertedCount, // 不存在则插入的document数量,
		updateManyResult.UpsertedID,
	)
	if err = sess.CommitTransaction(context.Background()); err != nil {
		return nil, err
	}
	return updateManyResult, nil
}

// 只支持集群
func WatchClass(ctx context.Context, client *mongo.Client, db *mongo.Database) {
	// 监控所有db中的所有collection的插入操作
	matchStage := bson.D{{"$match", bson.D{{"operationType", "insert"}}}}
	opts := options.ChangeStream().SetMaxAwaitTime(2 * time.Second)
	changeStream, err := client.Watch(ctx, mongo.Pipeline{matchStage}, opts)
	if err != nil {
		panic(err)
	}
	for changeStream.Next(ctx) {
		fmt.Println(changeStream.Current)
	}
}

// 聚合
func Aggregate(ctx context.Context, db *mongo.Database) {
	// 相当于select classname , count(1) from class group by classname
	//groupStage := bson.D{
	//	{"$group", bson.D{
	//		{"_id", "classname"},
	//		{"count", bson.D{
	//			{"$sum", 1},
	//		}},
	//	}},
	//}
	// 相当于 select classname, max(years) from class group by classname
	// 如果要对嵌套类型字段StuCount分组，就是$years改为$classdesc.stucount就行了
	groupStage := bson.D{
		{"$group", bson.D{
			{"_id", "$classname"},
			{"y", bson.D{
				{"$max", "$years"},
			}},
		}},
	}

	opts := options.Aggregate().SetMaxTime(2 * time.Second)
	aggCursor, err := db.Collection("class").Aggregate(ctx, mongo.Pipeline{groupStage}, opts)
	if err != nil {
		log.Fatal(err)
	}

	var results []bson.M
	if err = aggCursor.All(ctx, &results); err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Println(utils.ToJson(result))
	}
}

func CreateIndexes(ctx context.Context, db *mongo.Database) {
	models := []mongo.IndexModel{
		//{
		//	Keys:    bson.D{{"Classid", 1}, {"Classname", 1}},
		//	Options: options.Index().SetName("nameEmail"), // 指定索引名
		//},
		{ //创建一个全文索引
			Keys:    bson.D{{"Brief", "text"}},
			Options: options.Index().SetName("briefIdx"),
		},
	}

	opts := options.CreateIndexes().SetMaxTime(2 * time.Second)
	names, err := db.Collection("class").Indexes().CreateMany(context.TODO(), models, opts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("created indexes %v\n", names)
}

func FindCommentByFullTextIndex(ctx context.Context, db *mongo.Database) {
	//  db.getCollection("class").find({$text:{$search:"test"}})
	filter := &bson.D{
		{
			"$text", bson.M{
			//"$search": "wqD", // 如果要查询多个关键字就用空格分隔，含有一种一个就能返回
			//"$search": "\"ewtrwefs\" \"wqD\" ", // 查询提示包含这些关键字的document
			"$search": "\"ewtrwefs\" \"wqD\" -A", // 查询提示包含这些关键字,但不包含A的document
		},
		},
	}
	// 输出全文检索匹配的分数
	opts := options.Find().SetProjection(bson.D{{"score", bson.D{{"$meta", "textScore"}}}})
	// 英文的索引建立都是根据空格来切分的，为单词建立索引，中文也是按空格拆分而不是一个汉字来拆分的，所以对中文不太友好
	findCursor, err := db.Collection("class").Find(ctx, filter, opts)
	if err != nil {
		panic(err)
	}
	defer findCursor.Close(ctx)
	for findCursor.Next(context.TODO()) {
		// 创建一个值，将单个文档解码为该值
		var (
			elem model.Class
			e    bson.M
		)
		err := findCursor.Decode(&elem)
		err = findCursor.Decode(&e)
		if err != nil {
			fmt.Println(err.Error()+" id:", findCursor.ID())
			continue
		}
		fmt.Println(e)
		fmt.Println(utils.ToJson(elem))
	}
}

func ListIndex(ctx context.Context, db *mongo.Database) {
	cursor, _ := db.Collection("class").Indexes().List(ctx)
	defer cursor.Close(ctx)
	for cursor.Next(context.TODO()) {
		// 创建一个值，将单个文档解码为该值
		var elem []bson.M
		err := cursor.Decode(&elem)
		if err != nil {
			continue
		}
		fmt.Println(utils.ToJson(elem))
	}
}
