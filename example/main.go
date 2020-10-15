package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/ioswarm/golik"
	mongo "github.com/ioswarm/golik-mongo"
	"github.com/ioswarm/golik/db"
)

type Person struct {
	Email string
	Name string
	Age int
}

func main() {
	sys, err := golik.NewSystem("mongo-example")
	if err != nil {
		log.Fatalln(err)
	}

	mgo, err := mongo.Mongo(sys)
	if err != nil {
		log.Fatalln(err)
	}

	pool, err := mgo.CreateConnectionPool(&db.ConnectionPoolSettings{
		Name: "person",
		Type: reflect.TypeOf(Person{}),
		PoolSize: 10,
	})
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("Got:", <-pool.Request(context.Background(), db.Create(&Person{
		Email: "test@test.de",
		Name: "Test Testamnn",
		Age: 17,
	})))

	fmt.Println("Got:", <-pool.Request(context.Background(), db.Get("test@test.de")))
	fmt.Println("Got:", <-pool.Request(context.Background(), db.Delete("test@test.de")))

	os.Exit(<-sys.Terminated())
}
