package mongo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)
type MongoDI interface {
	NewMongoDBClient(ctx context.Context, userDB string) (MongoDBClient, error)
	GetConf()
	SetAuth(user,pwd string)
	GetUri() string
}
type MongoConf struct{
	Uri 	  string `yaml:"uri"`
	User	  string `yaml:"user"`
	Pass      string `yaml:"pass"`
	Defaul 	  string `yaml:"defaul"`

	authUri   string
}
func (mc *MongoConf)GetConf(){
	path,err := os.Getwd()
	if err!=nil{
		panic(err)
	}
	vip:=viper.New()
	vip.AddConfigPath(path+"/config")
	vip.SetConfigName("app")
	vip.SetConfigType("yaml")
	if err := vip.ReadInConfig(); err!=nil{
		panic(err)
	}
	err = vip.UnmarshalKey("mongo",&mc)
	if err != nil{
		panic(err)
	}
}
func (mc *MongoConf) SetAuth(user,pwd string){
	mc.GetConf()
	uriSpl := strings.Split(mc.Uri, "//")
	if len(uriSpl) >=2{
		if user != "" && pwd != ""{
			mc.authUri = uriSpl[0]+user+":"+pwd+"@"+uriSpl[1]
		}else{
			mc.authUri = uriSpl[0]+mc.User+":"+mc.Pass+"@"+uriSpl[1]
		}
	}
	fmt.Println("Uri input error")
}
func(mc *MongoConf) GetUri()string{
	mc.GetConf()
	if mc.authUri != ""{
		return mc.authUri
	}
	return mc.Uri
}
func(mc *MongoConf)NewMongoDBClient(ctx context.Context, userDB string)(MongoDBClient, error){
	if mc.Uri == "" {
		panic("mongo uri not set")
	}
	if mc.Defaul == "" {
		panic("mongo default db not set")
	}
	client, err := mongo.NewClient(options.Client().ApplyURI(mc.GetUri()))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	err = client.Ping(ctx,readpref.Primary())
		if err != nil {
		cancel()
		return nil, err
	}
		return &mgoClientImpl{
		clt:       client,
		ctx:       ctx,
		cancel:    cancel,
		defaultDB: mc.Defaul,
		userDB:    userDB,
	}, nil
}
type mgoClientImpl struct {
	clt     *mongo.Client
	ctx     context.Context
	cancel  context.CancelFunc
	session mongo.Session
	dbPool  map[string]*mongo.Database
	defaultDB string
	userDB    string
}
func (m *mgoClientImpl)WithSession(f func(sc mongo.SessionContext) error) error{
	if m.session!=nil{
		return nil
	}
	session,err:=m.clt.StartSession()
	if err != nil{
		m.cancel()
		return err
	}
	if err:=session.StartTransaction();err!=nil{
		return err
	}
	m.session = session
	return mongo.WithSession(m.ctx, m.session, f)
}
func (m *mgoClientImpl) GetDBList() ([]string, error) {
	return m.clt.ListDatabaseNames(m.ctx, bson.M{})
}

func (m *mgoClientImpl) getDB(db string) *mongo.Database {
	if db == "" {
		db = m.defaultDB
	}
	if dbclt, ok := m.dbPool[db]; ok {
		return dbclt
	}
	dbclt := m.clt.Database(db)
	m.dbPool[db] = dbclt
	return dbclt
}

func (m *mgoClientImpl) Close() {
	if m == nil {
		return
	}
	if m.session != nil {
		m.session.EndSession(m.ctx)
	}
	if m.clt != nil {
		err := m.clt.Disconnect(m.ctx)
		if err != nil {
			fmt.Println("disconnect error: " + err.Error())
		}
	}
	m.cancel()
}

func (m *mgoClientImpl) Ping() error {
	return m.clt.Ping(m.ctx, readpref.Primary())
}

func (m *mgoClientImpl) GetCoreDB() *mongo.Database {
	return m.getDB(m.defaultDB)
}

func (m *mgoClientImpl) GetUserDB() *mongo.Database {
	if m.userDB == "" {
		return nil
	}
	return m.getDB(m.userDB)
}

func (m *mgoClientImpl) AbortTransaction(sc mongo.SessionContext) error {
	return m.session.AbortTransaction(sc)
}

func (m *mgoClientImpl) CommitTransaction(sc mongo.SessionContext) error {
	return m.session.CommitTransaction(sc)
}

type MongoDBClient interface {
	GetCoreDB() *mongo.Database
	GetUserDB() *mongo.Database
	WithSession(f func(sc mongo.SessionContext) error) error
	AbortTransaction(sc mongo.SessionContext) error
	CommitTransaction(sc mongo.SessionContext) error
	Close()
	Ping() error
}