package dao

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type DocInter interface {
	GetC() string
	GetDoc() interface{}
	GetID() interface{}
	SetCreator(u LogUser)
	AddRecord(u LogUser,msg string) []*Record
	GetIndexes() []mongo.IndexModel
}

type LogUser interface{
	GetName() string
	GetAccount() string
}

func NewRecord(date time.Time,sum string,acc string,name string)*Record{
	return &Record{
		DataTime: date,
		Summary: sum,
		Account: acc,
		Name: name,
	}
}

type Record struct{
	DataTime time.Time
	Summary string
	Account string
	Name string
}