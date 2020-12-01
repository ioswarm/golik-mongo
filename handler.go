package mongo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ioswarm/golik"
	"go.mongodb.org/mongo-driver/bson"
	mgo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func defaultHandlerCreation(collection *mgo.Collection, itype reflect.Type, indexField string, behavior interface{}, rule ...golik.ConvertRule) golik.HandlerCreation {
	return func(ctx golik.CloveContext) (golik.Handler, error) {
		return NewMongoHandler(collection, itype, indexField, behavior, rule...)
	}
}

func NewMongoHandler(collection *mgo.Collection, itype reflect.Type, indexField string, behavior interface{}, rule ...golik.ConvertRule) (golik.Handler, error) {
	if collection == nil {
		return nil, errors.New("Collection is not defined [nil]")
	}
	if itype.Kind() != reflect.Struct {
		return nil, errors.New("Given type must be a struct")
	}

	fld := indexField
	if indexField == "" {
		if itype.NumField() == 0 {
			return nil, errors.New("Given type has no fields")
		}
		ftype := itype.Field(0)
		fld = golik.CamelCase(ftype.Name)
	}

	return &mongoHandler{
		collection: collection,
		itype:      itype,
		indexField: fld,
		converter:  golik.NewConverter().NameMapping(fld, "_id").AddRule(TimestampRule(), TimeRule()).AddRule(rule...),
		behavior:   behavior,
	}, nil
}

type mongoHandler struct {
	collection *mgo.Collection
	itype      reflect.Type
	indexField string
	converter  golik.Converter
	behavior   interface{}
}

func (h *mongoHandler) decode(i interface{}) (bson.M, error) {
	rmap, err := h.converter.ToMap(i)
	if err != nil {
		return nil, err
	}
	return bson.M(rmap), nil
}

func (h *mongoHandler) encode(bson bson.M) (interface{}, error) {
	ptrvalue := reflect.New(h.itype)
	if err := h.converter.Encode(bson, ptrvalue); err != nil {
		return nil, err
	}
	return ptrvalue.Interface(), nil
}

func (h *mongoHandler) Filter(ctx golik.CloveContext, flt *golik.Filter) (*golik.Result, error) {
	cond, err := flt.Condition()
	if err != nil {
		return nil, err // TODO
	}
	mfilter, err := NewFilter(cond)
	if err != nil {
		return nil, err
	}

	ctx.Debug("Excute filter: %v", mfilter)

	opts := options.Find()
	opts.SetSkip(int64(flt.From))
	opts.SetLimit(int64(flt.Size))

	timeout, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	count, _ := h.collection.CountDocuments(context.Background(), mfilter)

	cursor, err := h.collection.Find(context.Background(), mfilter, opts)
	defer cursor.Close(timeout)
	if err != nil {
		return nil, err
	}
	results := make([]interface{}, 0)
	for cursor.Next(timeout) {
		var res bson.M
		if err := cursor.Decode(&res); err != nil {
			return nil, err
		}

		value, err := h.encode(res)
		if err != nil {
			return nil, err
		}

		results = append(results, value)
	}

	return &golik.Result{
		From:   flt.From,
		Size:   len(results),
		Count:  int(count), // TODO
		Result: results,
	}, nil
}

func (h *mongoHandler) Create(ctx golik.CloveContext, cmd *golik.CreateCommand) error {
	bson, err := h.decode(cmd.Entity)
	if err != nil {
		return err
	}

	_, err = h.collection.InsertOne(ctx, bson)

	return err
}

func (h *mongoHandler) Read(ctx golik.CloveContext, cmd *golik.GetCommand) (interface{}, error) {
	if cmd.Id == nil {
		return nil, errors.New("Give id is nil")
	}
	flt := &golik.Filter{Filter: fmt.Sprintf("_id eq %v", cmd.Id), Size: 1}
	if str, ok := cmd.Id.(string); ok {
		flt.Filter = fmt.Sprintf("_id eq '%v'", str)
	}

	result, err := h.Filter(ctx, flt)
	if err != nil {
		return nil, err
	}
	if result.Size > 0 {
		return result.Result[0], nil
	}

	return nil, fmt.Errorf("Could not find entity with id %v", cmd.Id) // TODO define default errors
}

func (h *mongoHandler) Update(ctx golik.CloveContext, cmd *golik.UpdateCommand) error {
	data, err := h.decode(cmd.Entity)
	if err != nil {
		return err
	}

	_, err = h.collection.UpdateOne(ctx, bson.M{"_id": cmd.Id}, bson.M{"$set": data})
	return err
}

func (h *mongoHandler) Delete(ctx golik.CloveContext, cmd *golik.DeleteCommand) (interface{}, error) {
	data, err := h.Read(ctx, &golik.GetCommand{Id: cmd.Id})
	if err != nil {
		return nil, err
	}

	_, err = h.collection.DeleteOne(ctx, bson.M{"_id": cmd.Id})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (h *mongoHandler) OrElse(ctx golik.CloveContext, msg golik.Message) {
	if h.behavior != nil {
		ctx.AddOption("mongo.client", h.collection.Database().Client())
		ctx.AddOption("mongo.database", h.collection.Database())
		ctx.AddOption("mongo.collection", h.collection)
		golik.CallBehavior(ctx, msg, h.behavior)
	}
}
