package main

import (
	"container/heap"
	"fmt"
	"math"
)

type AggregateOrder struct {
	CustomerQuantities map[string]int
	FirmQuantity       int
	TotalQuantity      int
	CurrentPrice       int
	CurrentTime        int
}

func NewAggregateOrder() *AggregateOrder {
	return &AggregateOrder{
		CustomerQuantities: make(map[string]int),
	}
}

type Quote struct {
	CurrentPrice int
	CurrentTime  int
}

type Order struct {
	ID          string
	TargetPrice int
	Quantity    int
	CurrentTime int
	CutoffTime  int
}

type OrderQueue []*Order

func (pq OrderQueue) Len() int { return len(pq) }

func (pq OrderQueue) Less(i, j int) bool {
	return pq[i].CutoffTime < pq[j].CutoffTime
}

func (pq OrderQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *OrderQueue) Push(x interface{}) {
	item := x.(*Order)
	*pq = append(*pq, item)
}

func (pq *OrderQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func aggregateOrdersSolution(messages []interface{}) []*AggregateOrder {
	aggregateOrders := []*AggregateOrder{}
	ineligibleOrders := make(map[string]*Order)
	var currentPrice, currentTime int
	queue := make(OrderQueue, 0)
	pendingOrders := make([]*Order, 0)

	for i := 0; i < len(messages); {
		switch m := messages[i].(type) {
		case *Quote:
			currentPrice = m.CurrentPrice
			currentTime = m.CurrentTime
			i++
		case *Order:
			for i < len(messages) {
				if o, ok := messages[i].(*Order); ok {
					heap.Push(&queue, o)
					i++
				} else {
					break
				}
			}
			execute(&queue, &aggregateOrders, ineligibleOrders, &pendingOrders, currentPrice, currentTime)
		}
	}

	if len(queue) > 0 {
		execute(&queue, &aggregateOrders, ineligibleOrders, &pendingOrders, currentPrice, currentTime)
	}

	if len(ineligibleOrders) > 0 {
		runningQty := 0
		customerQuantities := make(map[string]int)
		for _, order := range ineligibleOrders {
			if currentTime >= order.CurrentTime && int(math.Abs(float64(currentPrice-order.TargetPrice))) <= 10 {
				runningQty += order.Quantity
				customerQuantities[order.ID] = order.Quantity
			}
		}

		aggregateOrders = append(aggregateOrders, &AggregateOrder{
			CustomerQuantities: customerQuantities,
			FirmQuantity:       100 - runningQty,
			TotalQuantity:      100,
			CurrentPrice:       currentPrice,
			CurrentTime:        currentTime,
		})
	}

	return aggregateOrders
}

func checkPendingOrders(pendingOrders []*Order, quotePrice, quoteTime int, queue *OrderQueue, ineligibleOrders map[string]*Order) {
	if len(pendingOrders) > 0 {
		for _, p := range pendingOrders {
			if quoteTime >= p.CutoffTime {
				heap.Push(queue, p)
			}
		}
		pendingOrders = pendingOrders[:0]
	}
}

func execute(queue *OrderQueue, aggregateOrders *[]*AggregateOrder, ineligibleOrders map[string]*Order, pendingOrders *[]*Order, quotePrice, quoteTime int) {
	runningQuantity := 0
	for queue.Len() > 0 {
		item := heap.Pop(queue).(*Order)
		order := *item
		// cutoffTime := order.CutoffTime

		if order.Quantity >= 100 {
			order.Quantity -= 100
			if orderIsEligible(quotePrice, quoteTime, &order) {
				*aggregateOrders = append(*aggregateOrders, &AggregateOrder{
					CustomerQuantities: map[string]int{order.ID: 100},
					TotalQuantity:      100,
					CurrentPrice:       quotePrice,
					CurrentTime:        order.CurrentTime,
				})
				if order.Quantity > 0 {
					heap.Push(queue, &order)
				}
			} else {
				if int(math.Abs(float64(quotePrice-order.TargetPrice))) > 10 {
					ineligibleOrders[order.ID] = &order
					continue
				}
				heap.Push(queue, &order)
			}
		} else {
			if order.CutoffTime >= quoteTime {
				if orderIsEligible(quotePrice, quoteTime, &order) {
					runningQuantity += order.Quantity
					*pendingOrders = append(*pendingOrders, &order)
				} else {
					ineligibleOrders[order.ID] = &order
					continue
				}

				if runningQuantity == 100 {
					customerQuantities := make(map[string]int)
					for _, po := range *pendingOrders {
						customerQuantities[po.ID] = po.Quantity
					}
					*aggregateOrders = append(*aggregateOrders, &AggregateOrder{
						CustomerQuantities: customerQuantities,
						TotalQuantity:      100,
						CurrentPrice:       quotePrice,
						CurrentTime:        order.CurrentTime,
					})
					runningQuantity = 0
					*pendingOrders = (*pendingOrders)[:0]
				} else if runningQuantity > 100 {
					totalQuantity := 0
					customerQuantities := make(map[string]int)
					for _, p := range *pendingOrders {
						totalQuantity += p.Quantity
						if totalQuantity > 100 {
							delta := totalQuantity - 100
							customerQuantities[p.ID] = p.Quantity - delta
							p.Quantity = delta
							heap.Push(queue, p)
							*aggregateOrders = append(*aggregateOrders, &AggregateOrder{
								CustomerQuantities: customerQuantities,
								TotalQuantity:      100,
								CurrentPrice:       quotePrice,
								CurrentTime:        order.CurrentTime,
							})
							runningQuantity = 0
							*pendingOrders = (*pendingOrders)[:0]
						} else {
							customerQuantities[p.ID] = p.Quantity
						}
					}
				}
			} else {
				*aggregateOrders = append(*aggregateOrders, &AggregateOrder{
					CustomerQuantities: map[string]int{order.ID: order.Quantity},
					TotalQuantity:      100,
					FirmQuantity:       100 - order.Quantity,
					CurrentPrice:       quotePrice,
					CurrentTime:        order.CurrentTime,
				})
				*pendingOrders = (*pendingOrders)[:0]
			}
		}
	}

	for _, i := range *pendingOrders {
		if i.CutoffTime >= quoteTime {
			*aggregateOrders = append(*aggregateOrders, &AggregateOrder{
				CustomerQuantities: map[string]int{i.ID: i.Quantity},
				TotalQuantity:      100,
				FirmQuantity:       100 - i.Quantity,
				CurrentPrice:       quotePrice,
				CurrentTime:        i.CurrentTime,
			})
		}
	}
	*pendingOrders = (*pendingOrders)[:0]
}

func orderIsEligible(quotePrice, quoteTime int, order *Order) bool {
	return quoteTime <= order.CutoffTime && int(math.Abs(float64(quotePrice-order.TargetPrice))) < 10
}

func main() {
	messages := []interface{}{
		&Quote{CurrentPrice: 10, CurrentTime: 1},
		&Order{ID: "0", TargetPrice: 10, Quantity: 100, CurrentTime: 2, CutoffTime: 10},
		&Order{ID: "1", TargetPrice: 10, Quantity: 300, CurrentTime: 3, CutoffTime: 10},
		&Quote{CurrentPrice: 12, CurrentTime: 4},
		&Order{ID: "2", TargetPrice: 10, Quantity: 20, CurrentTime: 4, CutoffTime: 10},
		&Order{ID: "3", TargetPrice: 10, Quantity: 30, CurrentTime: 5, CutoffTime: 10},
		&Order{ID: "4", TargetPrice: 10, Quantity: 50, CurrentTime: 6, CutoffTime: 10},
		&Order{ID: "5", TargetPrice: 10, Quantity: 130, CurrentTime: 7, CutoffTime: 10},
		&Order{ID: "6", TargetPrice: 10, Quantity: 80, CurrentTime: 10, CutoffTime: 10},
		&Order{ID: "7", TargetPrice: 23, Quantity: 10, CurrentTime: 11, CutoffTime: 11},
		&Order{ID: "8", TargetPrice: 1, Quantity: 20, CurrentTime: 11, CutoffTime: 11},
		&Order{ID: "9", TargetPrice: 9, Quantity: 70, CurrentTime: 11, CutoffTime: 11},
		&Order{ID: "10", TargetPrice: 30, Quantity: 50, CurrentTime: 12, CutoffTime: 20},
		&Quote{CurrentPrice: 33, CurrentTime: 12},
	}

	expectedOrders := []*AggregateOrder{
		{CustomerQuantities: map[string]int{"0": 100}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 10, CurrentTime: 2},
		{CustomerQuantities: map[string]int{"1": 100}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 10, CurrentTime: 3},
		{CustomerQuantities: map[string]int{"1": 100}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 10, CurrentTime: 3},
		{CustomerQuantities: map[string]int{"1": 100}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 10, CurrentTime: 3},
		{CustomerQuantities: map[string]int{"2": 20, "3": 30, "4": 50}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 12, CurrentTime: 6},
		{CustomerQuantities: map[string]int{"5": 100}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 12, CurrentTime: 7},
		{CustomerQuantities: map[string]int{"5": 30, "6": 70}, FirmQuantity: 0, TotalQuantity: 100, CurrentPrice: 12, CurrentTime: 10},
		{CustomerQuantities: map[string]int{"6": 10}, FirmQuantity: 90, TotalQuantity: 100, CurrentPrice: 12, CurrentTime: 10},
		{CustomerQuantities: map[string]int{"9": 70}, FirmQuantity: 30, TotalQuantity: 100, CurrentPrice: 12, CurrentTime: 11},
		{CustomerQuantities: map[string]int{"7": 10, "10": 50}, FirmQuantity: 40, TotalQuantity: 100, CurrentPrice: 33, CurrentTime: 12},
	}

	orders := aggregateOrdersSolution(messages)
	minNumOrders := min(len(orders), len(expectedOrders))
	for i := 0; i < minNumOrders; i++ {
		if ordersMatch(orders[i], expectedOrders[i]) {
			fmt.Printf("Order %d: ok\n", i)
		} else {
			fmt.Printf("Order %d: expected %v, got %v\n", i, expectedOrders[i], orders[i])
			fmt.Println("TEST FAILED")
			return
		}
	}

	if len(orders) > len(expectedOrders) {
		fmt.Println("Extra orders:")
		for _, order := range orders[minNumOrders:] {
			fmt.Println(order)
		}
		fmt.Println("TEST FAILED")
	} else if len(orders) < len(expectedOrders) {
		fmt.Println("Missing orders:")
		for _, order := range expectedOrders[minNumOrders:] {
			fmt.Println(order)
		}
		fmt.Println("TEST FAILED")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func ordersMatch(order1, order2 *AggregateOrder) bool {
	if order1.FirmQuantity != order2.FirmQuantity ||
		order1.CurrentPrice != order2.CurrentPrice ||
		order1.CurrentTime != order2.CurrentTime ||
		order1.TotalQuantity != order2.TotalQuantity {
		return false
	}
	if len(order1.CustomerQuantities) != len(order2.CustomerQuantities) {
		return false
	}
	for orderID, quantity := range order1.CustomerQuantities {
		if order2.CustomerQuantities[orderID] != quantity {
			return false
		}
	}
	return true
}
