package main

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)
func isCollectExisted(db *mongo.Database) []string {
	names, err := db.ListCollectionNames(context.Background(), bson.D{{Key: "name", Value: "test1"}})
	if err != nil{
		fmt.Println(err)
	}
	fmt.Println(reflect.TypeOf(names))
	return names
	// if ce, ok := err.(mongo.CommandError); ok {
	// 	if ce.Name == "OperationNotSupportedInTransaction" {
	// 		return true
	// 	}
	// 	return false
	// }

}

func main(){
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err!= nil{
		fmt.Println(err)
	}
	db:=client.Database("demo")
	result := isCollectExisted(db)
	for _,value := range result{
		fmt.Println(value)
	}
}