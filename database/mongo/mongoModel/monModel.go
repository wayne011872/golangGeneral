package mongoModel

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/wayne011872/golangGeneral/dao"
	"github.com/wayne011872/golangGeneral/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MgoAggregate interface{
	GetPipeLine(q bson.M) mongo.Pipeline
	GetC() string
}

type MgoDBModel interface{
	DisableCheckBeforeSave(b bool)
	SetDB(db *mongo.Database)
	Save(d dao.DocInter,u dao.LogUser)(interface{},error)
	BatchSave(doclist []dao.DocInter,u dao.LogUser) (inserted []interface{}, failed []dao.DocInter, err error)
	FindById(d dao.DocInter)error
	FindOne(d dao.DocInter,q bson.M, option ...*options.FindOneOptions) error
	Find(d dao.DocInter,q bson.M,option ...*options.FindOptions)(interface{},error)
	FindAndExec(d dao.DocInter,q bson.M,exec func(i interface{})error,opts ...*options.FindOptions)error
	PipeFindOne(aggr MgoAggregate,filter bson.M)error
	PipeFind(aggr MgoAggregate,filter bson.M,opts ...*options.AggregateOptions) (interface{},error)
	PipeFindAndExec(aggr MgoAggregate,q bson.M,exec func(i interface{})error,opts ...*options.AggregateOptions)error
	UpdateOne(d dao.DocInter,field bson.D,u dao.LogUser)(int64,error)
	UpdateAll(d dao.DocInter,q bson.M,field bson.D,u dao.LogUser)(int64,error)
	RemoveByID(d dao.DocInter,u dao.LogUser)(int64,error)
	RemoveAll(d dao.DocInter,q bson.M,u dao.LogUser)(int64,error)
}

func NewMgoModel(ctx context.Context, db *mongo.Database, log log.Logger)MgoDBModel{
	return &mgoModelImpl{
		db: db,
		ctx: ctx,
		selfCtx: context.Background(),
		log: log,
	}
}

type mgoModelImpl struct{
	disableCheckBeforeSave bool
	db			*mongo.Database
	log			log.Logger
	ctx			context.Context

	selfCtx		context.Context
	indexExistMap map[string]bool
}
func (mm *mgoModelImpl) DisableCheckBeforeSave(b bool) {
	mm.disableCheckBeforeSave = b
}

func (mm *mgoModelImpl) SetDB(db *mongo.Database) {
	mm.db = db
}

func (mm *mgoModelImpl) CountDocuments(d dao.DocInter,q bson.M) (int64,error)	{
	opts := options.Count().SetMaxTime(2 *time.Second)
	return mm.db.Collection(d.GetC()).CountDocuments(mm.ctx,q,opts)
}

func (mm *mgoModelImpl) IsCollectExisted(d dao.DocInter) bool{
	names,err := mm.db.ListCollectionNames(mm.selfCtx,bson.D{{Key:"names",Value:d.GetC()}})
	if ce, ok := err.(mongo.CommandError); ok {
		if ce.Name == "OperationNotSupportedInTransaction" {
			return true
		}
		return false
	}
	if len(names) == 0{
		return false
	}
	return true
}

func (mm *mgoModelImpl) CreateCollection(dlist ...dao.DocInter) (err error){
	for _,d:= range dlist {
		mm.log.Info("check collection "+d.GetC())
		if !mm.IsCollectExisted(d) {
			err = mm.db.CreateCollection(mm.ctx,d.GetC())
			if err != nil{
				mm.log.Warn(fmt.Sprintf("created collection [%s] fail: %s",d.GetC(),err.Error()))
			} else{
				mm.log.Info("collection created: "+d.GetC())
			}
		}
	}
	return
}

func (mm *mgoModelImpl) Save(d dao.DocInter,u dao.LogUser)(interface{},error){
	if u!=nil {
		d.SetCreator(u)
	}
	collection := mm.db.Collection(d.GetC())

	result,err:=collection.InsertOne(mm.ctx,d.GetDoc())
	if err !=nil{
		return primitive.NilObjectID,err
	}
	return result.InsertedID,err
}

func (mm *mgoModelImpl) BatchSave(doclist []dao.DocInter,u dao.LogUser)(inserted []interface{}, failed []dao.DocInter,err error){
	if len(doclist) == 0{
		inserted = nil
		return 
	}
	ordered := false
	var batch [] interface{}
	for _,d := range doclist{
		if u != nil{
			d.SetCreator(u)
		}
		batch = append(batch, d)
	}
	collection := mm.db.Collection(doclist[0].GetC())
	var result *mongo.InsertManyResult
	result,err = collection.InsertMany(mm.ctx, batch, &options.InsertManyOptions{Ordered: &ordered})
	if result != nil{
		inserted = result.InsertedIDs
	}

	if except,ok := err.(mongo.BulkWriteException); ok{
		for _ , e := range except.WriteErrors{
			failed = append(failed, doclist[e.Index])
		}
	}
	return
}

func (mm *mgoModelImpl) FindOne(d dao.DocInter,q bson.M,option ...*options.FindOneOptions) error{
	if mm.db == nil{
		return errors.New("db is null")
	}
	if d == nil{
		return errors.New("document is null")
	}
	collection := mm.db.Collection(d.GetC())
	return collection.FindOne(mm.ctx,q,option...).Decode(d)
}

func (mm *mgoModelImpl) FindById(d dao.DocInter)error{
	return mm.FindOne(d,bson.M{"_id":d.GetID()})
}

func (mm *mgoModelImpl) Find(d dao.DocInter,q bson.M, option ...*options.FindOptions)(interface{},error){
	myType := reflect.TypeOf(d)
	slice := reflect.MakeSlice(reflect.SliceOf(myType),0,0).Interface()
	collection := mm.db.Collection(d.GetC())
	sortCursor ,err:= collection.Find(mm.ctx,q,option...)
	if err != nil{
		return nil,err
	}
	err = sortCursor.All(mm.ctx,&slice)
	if err !=nil {
		return nil,err
	}
	return slice,err
}

func (mm *mgoModelImpl) PipeFind(aggr MgoAggregate,filter bson.M,opts ...*options.AggregateOptions)(interface{},error) {
	myType := reflect.TypeOf(aggr)
	slice := reflect.MakeSlice(reflect.SliceOf(myType),0,0).Interface()
	collection := mm.db.Collection(aggr.GetC())
	sortCursor,err := collection.Aggregate(mm.ctx,aggr.GetPipeLine(filter),opts...)
	if err != nil{
		return nil,err
	}
	err = sortCursor.All(mm.ctx,&slice)
	if err != nil{
		return nil,err
	}
	return slice,err
}
func (mm *mgoModelImpl) PipeFindAndExec(aggr MgoAggregate,filter bson.M,exec func(i interface{})error,opts ...*options.AggregateOptions)error{
	collection := mm.db.Collection(aggr.GetC())
	sortCursor, err := collection.Aggregate(mm.ctx,aggr.GetPipeLine(filter),opts...)
	if err != nil{
		return err
	}
	val := reflect.ValueOf(aggr)
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}
	var newValue reflect.Value
	var newDoc dao.DocInter
	for sortCursor.Next(mm.ctx) {
		newValue = reflect.New(val.Type())
		newDoc = newValue.Interface().(dao.DocInter)
		err = sortCursor.Decode(newDoc)
		if err != nil{
			return err
		}
		err = exec(newDoc)
		if err != nil{
			return err
		}
	}

	w2 := reflect.ValueOf(newValue)
	if w2.IsZero() {
		return nil
	}
	for i := 0; i < val.NumField(); i++{
		f := val.Field(i)
		f.Set(newValue.Elem().Field(i))
	}
	return err
}

func (mm *mgoModelImpl) FindAndExec(d dao.DocInter,
	q bson.M, 
	exec func(i interface{})error, 
	opts ...*options.FindOptions,
)error{
	var err error
	collection := mm.db.Collection(d.GetC())
	sortCursor, err := collection.Find(mm.ctx,q,opts...)
	if err != nil{
		return nil
	}
	val := 	reflect.ValueOf(d)
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}
	var newValue reflect.Value
	var newDoc dao.DocInter
	for sortCursor.Next(mm.ctx) {
		newValue = reflect.New(val.Type())
		newDoc = newValue.Interface().(dao.DocInter)
		err = sortCursor.Decode(newDoc)
		if err != nil {
			return err
		}
		err = exec(newDoc)
		if err !=nil {
			return err
		}
	}
	w2 := reflect.ValueOf(newValue)
	if w2.IsZero() {
		return nil
	}
	for i := 0; i < val.NumField(); i++{
		f := val.Field(i)
		f.Set(newValue.Elem().Field(i))
	}
	return err
}

func (mm *mgoModelImpl) PipeFindOne(aggr MgoAggregate, filter bson.M)error{
	collection := mm.db.Collection(aggr.GetC())
	sortCursor, err := collection.Aggregate(mm.ctx,aggr.GetPipeLine(filter))
	if err != nil{
		return err
	}
	if sortCursor.Next(mm.ctx){
		err = sortCursor.Decode(aggr)
		if err != nil{
			return err
		}
	}
	return nil
}
func (mm *mgoModelImpl) UpdateOne(d dao.DocInter,fields bson.D,u dao.LogUser)(int64,error){
	if u != nil{
		fields = append(fields, primitive.E{Key:"records",Value: d.AddRecord(u,"updated")})
	}
	collection := mm.db.Collection(d.GetC())
	result,err:=collection.UpdateOne(mm.ctx,bson.M{"_id":d.GetID()},
		bson.D{
			{Key: "$set",Value: fields},
		},
	)
	if result != nil{
		return result.ModifiedCount,err
	}
	return 0,err
}

func (mm *mgoModelImpl) UpdateAll(d dao.DocInter,q bson.M,fields bson.D,u dao.LogUser)(int64,error){
	updated := bson.D{
		{Key: "$set", Value: fields},
	}
	if u != nil{
		updated = append(updated, primitive.E{Key: "$push",Value: primitive.M{"records":dao.NewRecord(time.Now(),u.GetAccount(),u.GetName(),"updated")}})
	}
	collection := mm.db.Collection(d.GetC())
	result,err := collection.UpdateMany(mm.ctx,q,updated)
	if result != nil{
		return result.ModifiedCount,err
	}
	return 0,err
}

func (mm *mgoModelImpl) RemoveByID(d dao.DocInter,u dao.LogUser) (int64,error){
	collection := mm.db.Collection(d.GetC())
	result,err:= collection.DeleteOne(mm.ctx,bson.M{"_id":d.GetID()})
	return result.DeletedCount,err
}

func (mm *mgoModelImpl) RemoveAll(d dao.DocInter,q bson.M,u dao.LogUser)(int64,error){
	collection := mm.db.Collection(d.GetC())
	result,err := collection.DeleteMany(mm.ctx,q)
	return result.DeletedCount ,err
}


