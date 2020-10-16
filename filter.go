package mongo

import (
	"fmt"

	"github.com/ioswarm/golik"
	"go.mongodb.org/mongo-driver/bson"
)

func NewFilter(cond golik.Condition) (bson.M, error) {
	return interpretCondition(cond)
}

func interpretCondition(condition golik.Condition) (bson.M, error) {
	switch condition.(type) {
	case golik.Operand:
		operand := condition.(golik.Operand)
		return interpretOperand(operand)
	case golik.Logic:
		logic := condition.(golik.Logic)
		return interpretLogical(logic)
	case golik.LogicNot:
		not := condition.(golik.LogicNot)
		return interpretNot(not)
	case golik.Grouping:
		grp := condition.(golik.Grouping)
		return interpretCondition(grp.InnerGroup())
	default:
		return nil, fmt.Errorf("Unsupported condition %T", condition)
	}
}

func interpretOperand(op golik.Operand) (bson.M, error) {
	switch op.Operator() {
	case golik.EQ:
		return bson.M{op.Attribute() : bson.M{"$eq" : op.Value() }}, nil
	case golik.NE:
		return bson.M{op.Attribute() : bson.M{"$ne" : op.Value() }}, nil
	case golik.CO:
		return bson.M{op.Attribute() : bson.M{"$regex" : fmt.Sprintf("/^.*%v.*$/", op.Value()) }}, nil
	case golik.SW:
		return bson.M{op.Attribute() : bson.M{"$regex" : fmt.Sprintf("/^%v/", op.Value()) }}, nil
	case golik.EW:
		return bson.M{op.Attribute() : bson.M{"$regex" : fmt.Sprintf("/%v$/", op.Value()) }}, nil
	case golik.PR:
		return bson.M{op.Attribute() : bson.M{"$exists" : true }}, nil
	case golik.GT:
		return bson.M{op.Attribute() : bson.M{"$gt" : op.Value() }}, nil
	case golik.GE:
		return bson.M{op.Attribute() : bson.M{"$gte" : op.Value() }}, nil
	case golik.LT:
		return bson.M{op.Attribute() : bson.M{"$lt" : op.Value() }}, nil
	case golik.LE:
		return bson.M{op.Attribute() : bson.M{"$lte" : op.Value() }}, nil
	default:
		return nil, fmt.Errorf("Unsupported operator %v", op.Operator())
	}
}

func interpretLogical(logic golik.Logic) (bson.M, error) {
	switch logic.Logical() {
	case golik.AND:
		l, err := interpretCondition(logic.Left())
		if err != nil {
			return nil, err
		}
		r, err := interpretCondition(logic.Right())
		if err != nil {
			return nil, err
		}
		return bson.M{"$and" : bson.A{l, r} }, nil
	case golik.OR:
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

func interpretNot(not golik.LogicNot) (bson.M, error) {
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
