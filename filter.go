package mongo

import (
	"fmt"

	"github.com/ioswarm/golik/filter"
	"go.mongodb.org/mongo-driver/bson"
)

func NewFilter(cond filter.Condition) (bson.M, error) {
	return interpretCondition(cond)
}

func interpretCondition(condition filter.Condition) (bson.M, error) {
	switch condition.(type) {
	case filter.Operand:
		operand := condition.(filter.Operand)
		return interpretOperand(operand)
	case filter.Logic:
		logic := condition.(filter.Logic)
		return interpretLogical(logic)
	case filter.LogicNot:
		not := condition.(filter.LogicNot)
		return interpretNot(not)
	case filter.Grouping:
		grp := condition.(filter.Grouping)
		return interpretCondition(grp.InnerGroup())
	default:
		return nil, fmt.Errorf("Unsupported condition %T", condition)
	}
}

func interpretOperand(op filter.Operand) (bson.M, error) {
	switch op.Operator() {
	case filter.EQ:
		return bson.M{op.Attribute() : bson.M{"$eq" : op.Value() }}, nil
	case filter.NE:
		return bson.M{op.Attribute() : bson.M{"$ne" : op.Value() }}, nil
	case filter.CO:
		return bson.M{op.Attribute() : bson.M{"$regex" : fmt.Sprintf("/^.*%v.*$/", op.Value()) }}, nil
	case filter.SW:
		return bson.M{op.Attribute() : bson.M{"$regex" : fmt.Sprintf("/^%v/", op.Value()) }}, nil
	case filter.EW:
		return bson.M{op.Attribute() : bson.M{"$regex" : fmt.Sprintf("/%v$/", op.Value()) }}, nil
	case filter.PR:
		return bson.M{op.Attribute() : bson.M{"$exists" : true }}, nil
	case filter.GT:
		return bson.M{op.Attribute() : bson.M{"$gt" : op.Value() }}, nil
	case filter.GE:
		return bson.M{op.Attribute() : bson.M{"$gte" : op.Value() }}, nil
	case filter.LT:
		return bson.M{op.Attribute() : bson.M{"$lt" : op.Value() }}, nil
	case filter.LE:
		return bson.M{op.Attribute() : bson.M{"$lte" : op.Value() }}, nil
	default:
		return nil, fmt.Errorf("Unsupported operator %v", op.Operator())
	}
}

func interpretLogical(logic filter.Logic) (bson.M, error) {
	switch logic.Logical() {
	case filter.AND:
		l, err := interpretCondition(logic.Left())
		if err != nil {
			return nil, err
		}
		r, err := interpretCondition(logic.Right())
		if err != nil {
			return nil, err
		}
		return bson.M{"$and" : bson.A{l, r} }, nil
	case filter.OR:
		l, err := interpretCondition(logic.Left())
		if err != nil {
			return nil, err
		}
		r, err := interpretCondition(logic.Right())
		if err != nil {
			return nil, err
		}
		return bson.M{"$or" : bson.A{l, r} }, nil
	default:
		return nil, fmt.Errorf("Unsupported logical operator %v", logic.Logical())
	}
}

func interpretNot(not filter.LogicNot) (bson.M, error) {
	inner, err := interpretCondition(not.InnerNot())
	if err != nil {
		return nil, err
	}
	result := bson.M{}
	for key := range inner {
		result[key] = bson.M{"$not" : inner[key] }
	}
	return result, nil
}
