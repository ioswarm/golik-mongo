package mongo

import (
	"fmt"
	"reflect"
	"time"

	"github.com/golang/protobuf/ptypes"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/ioswarm/golik"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	timeType      = reflect.TypeOf((*time.Time)(nil)).Elem()
	timestampType = reflect.TypeOf((*timestamppb.Timestamp)(nil)).Elem()
	dateTimeType  = reflect.TypeOf((*primitive.DateTime)(nil)).Elem()
)

type timestampRule struct{}

func TimestampRule() golik.ConvertRule {
	return &timestampRule{}
}

func (*timestampRule) Check(tpe reflect.Type) bool {
	return tpe == timestampType
}

func (*timestampRule) Decode(conv golik.Converter, value reflect.Value) (interface{}, error) {
	if ts, ok := value.Interface().(timestamppb.Timestamp); ok {
		return primitive.NewDateTimeFromTime(ts.AsTime()), nil
	}
	return nil, fmt.Errorf("Could not decode %T to primitive.DateTime", value.Interface())
}

func (*timestampRule) Encode(conv golik.Converter, i interface{}, value reflect.Value) error {
	switch i.(type) {
	case time.Time:
		time := i.(time.Time)
		ts, err := ptypes.TimestampProto(time)
		if err != nil {
			return err
		}
		value.Set(reflect.ValueOf(ts).Elem())
		return nil
	case timestamppb.Timestamp:
		value.Set(reflect.ValueOf(i))
		return nil
	case primitive.DateTime:
		dt := i.(primitive.DateTime)
		ts, err := ptypes.TimestampProto(dt.Time())
		if err != nil {
			return nil
		}
		value.Set(reflect.ValueOf(ts).Elem())
		return nil
	default:
		return fmt.Errorf("Could not encode %T to timestamppb.Timestamp", i)
	}
}

type timeRule struct{}

func TimeRule() golik.ConvertRule {
	return &timeRule{}
}

func (*timeRule) Check(tpe reflect.Type) bool {
	return tpe == timeType
}

func (*timeRule) Decode(conv golik.Converter, value reflect.Value) (interface{}, error) {
	if time, ok := value.Interface().(time.Time); ok {
		return primitive.NewDateTimeFromTime(time), nil
	}
	return nil, fmt.Errorf("Could not decode %T to primitive.DateTime", value.Interface())
}

func (*timeRule) Encode(conv golik.Converter, i interface{}, value reflect.Value) error {
	switch i.(type) {
	case time.Time:
		value.Set(reflect.ValueOf(i))
		return nil
	case primitive.DateTime:
		dt := i.(primitive.DateTime)
		value.Set(reflect.ValueOf(dt.Time()))
		return nil
	default:
		return fmt.Errorf("Could not encode %T to time.Time", i)
	}
}
