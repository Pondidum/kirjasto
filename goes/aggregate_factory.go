package goes

import "reflect"

type AggregateFactory map[string]func() Aggregate

func FactoryFor[TAggregate Aggregate](create func() TAggregate) (string, func() Aggregate) {
	name := reflect.TypeOf(*new(TAggregate)).Elem().Name()

	return name, func() Aggregate {
		return create()
	}
}
