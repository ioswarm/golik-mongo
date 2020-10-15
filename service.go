package mongo

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/ioswarm/golik"
	"github.com/ioswarm/golik/db"
	mgo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func Mongo(system golik.Golik) (*MongoService, error) {
	return NewMongo("mongo", system)
}

func NewMongo(name string, system golik.Golik) (*MongoService, error) {
	ms := &MongoService{
		name:     name,
		system:   system,
		settings: newSettings(name),
	}

	con, err := system.ExecuteService(ms)
	if err != nil {
		return nil, err
	}

	ms.mutex.Lock()
	defer ms.mutex.Unlock()
	ms.handler = con

	return ms, nil
}

type MongoService struct {
	name     string
	system   golik.Golik
	handler  golik.CloveHandler
	settings *Settings
	client   *mgo.Client
	database *mgo.Database

	mutex sync.Mutex
}

func (ms *MongoService) CreateServiceInstance(system golik.Golik) *golik.Clove {
	return &golik.Clove{
		Name: ms.name,
		Behavior: func(ctx golik.CloveContext, msg golik.Message) {
			msg.Reply(golik.Done())
		},
		PreStart: ms.connect,
		PostStop: ms.close,
	}
}

func (ms *MongoService) connect(ctx golik.CloveContext) error {
	ct, cancel := context.WithTimeout(context.Background(), ms.settings.ConnectionTimeout)
	client, err := mgo.Connect(ct, ms.settings.ClientOptions())
	defer cancel()
	if err != nil {
		return golik.Errorf("Could not create mongo-client connection to [%v]: %v", ms.settings.Uri, err)
	}

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return golik.Errorf("Could not connect to mongodb [%v]: %v", ms.settings.Uri, err)
	}

	ms.mutex.Lock()
	defer ms.mutex.Unlock()
	ctx.Info("Successfully connected to mongodb [%v]", ms.settings.Uri)
	ms.client = client
	ms.database = client.Database(ms.settings.Database)

	return nil
}

func (ms *MongoService) close(ctx golik.CloveContext) error {
	if ms.client == nil {
		return nil
	}
	err := ms.client.Disconnect(context.Background())
	if err != nil {
		return golik.Errorf("Could not disconnect from mongodb [%v]: %v", ms.settings.Uri, err)
	}
	ctx.Info("Disconnected from mongodb [%v]", ms.settings.Uri)
	return nil
}

func (ms *MongoService) Settings() *Settings {
	return ms.settings
}

func (ms *MongoService) Client() *mgo.Client {
	return ms.client
}

func (ms *MongoService) Database() *mgo.Database {
	return ms.database
}

func (ms *MongoService) CreateConnectionPool(settings *db.ConnectionPoolSettings) (golik.CloveRef, error) {
	if settings.Type.Kind() != reflect.Struct {
		return nil, errors.New("Given type must be a struct")
	}

	if settings.Options == nil {
		settings.Options = make(map[string]interface{})
	}
	if _, ok := settings.Options["mongo.client"]; !ok {
		settings.Options["mongo.client"] = ms.client
	}
	if _, ok := settings.Options["mongo.database"]; !ok {
		settings.Options["mongo.database"] = ms.database
	}
	if settings.CreateHandler == nil {
		getCollection := func() string {
			if value, ok := settings.Options["collection"]; ok {
				if name, ok := value.(string); ok {
					return name
				}
			}
			return settings.Name
		}

		collection := ms.database.Collection(getCollection())
		settings.Options["mongo.collection"] = collection
		settings.CreateHandler = defaultHandlerCreation(collection, settings.Type, settings.IndexField, settings.Behavior)
	}

	clove := db.NewConnectionPool(settings)
	return ms.handler.Execute(clove)
}
