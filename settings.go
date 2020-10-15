package mongo

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Settings struct {
	Poolsize int
	Database string
	Uri string
	ConnectionTimeout time.Duration
}

func (s *Settings) ClientOptions() *options.ClientOptions {
	return options.Client().ApplyURI(s.Uri)
}

func newBaseSettings() *Settings {
	return &Settings{
		Poolsize: viper.GetInt("mongo.poolsize"),
		Database: viper.GetString("mongo.database"),
		Uri: viper.GetString("mongo.uri"),
		ConnectionTimeout: time.Duration(viper.GetInt("mongo.connection-timeout")) * time.Second,
	}
}

func newSettings(name string) *Settings {
	bs := newBaseSettings()

	getPath := func(segment string) string {
		return fmt.Sprintf("mongo.%v.%v", name, segment)
	}

	path := getPath("poolsize")
	if viper.IsSet(path) {
		bs.Poolsize = viper.GetInt(path)
	}

	path = getPath("database")
	if viper.IsSet(path) {
		bs.Database = viper.GetString(path)
	}

	path = getPath("uri")
	if viper.IsSet(path) {
		bs.Uri = viper.GetString(path)
	}

	path = getPath("connection-timeout")
	if viper.IsSet(path) {
		bs.ConnectionTimeout = time.Duration(viper.GetInt(path)) * time.Second
	}

	return bs
}


func init() {
	viper.SetDefault("mongo.poolsize", 10)
	viper.SetDefault("mongo.database", "golik")
	viper.SetDefault("mongo.uri", "mongodb://localhost:27017")
	viper.SetDefault("mongo.connection-timeout", 10)
}