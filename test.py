from collections import namedtuple
import heapq

class AggregateOrder:
    def __init__(self, customer_quantities=None, firm_quantity=0, total_quantity=0, current_price=0, current_time=0):
        if customer_quantities is None:
            customer_quantities = {}
        self.customer_quantities = customer_quantities  # Dictionary mapping Order ID to quantity
        self.firm_quantity = firm_quantity  # Quantity joined by the firm
        self.total_quantity = total_quantity  # Total quantity in the aggregate order
        self.current_price = current_price  # Current price
        self.current_time = current_time  # Current time

    def __str__(self):
        return f"AggregateOrder(customer_quantities={self.customer_quantities}, firm_quantity={self.firm_quantity}, total_quantity={self.total_quantity}, current_price={self.current_price}, current_time={self.current_time})"


class Quote:
    def __init__(self, current_price, current_time):
        self.current_price = current_price
        self.current_time = current_time

class Order:
    def __init__(self, ID, target_price, quantity, current_time, cutoff_time):
        self.ID = ID
        self.target_price = target_price
        self.quantity = quantity
        self.current_time = current_time
        self.cutoff_time = cutoff_time
    
    def __lt__(self, other):
        return self.ID < other.ID


def aggregate_orders_solution(messages):
    aggregate_orders = []
    ineligible_orders = {}
    current_price, current_time = 0, 0
    queue = []
    pending_orders = []

    i = 0
    while i < len(messages):
        if isinstance(messages[i], Quote):
            current_price = messages[i].current_price
            current_time = messages[i].current_time
            i += 1
        elif isinstance(messages[i], Order):
            while i < len(messages) and isinstance(messages[i], Order):
                heapq.heappush(queue, (messages[i].cutoff_time, messages[i], current_price, current_time))
                i += 1
            execute(queue, aggregate_orders, ineligible_orders, pending_orders)

    if queue:
        execute(queue, aggregate_orders, ineligible_orders, pending_orders)
    
    if ineligible_orders:
        running_qty = 0
        customer_quantities = {}
        for order_id, order in ineligible_orders.items():
            if current_time >= order.current_time and abs(current_price - order.target_price) <= 10:
                running_qty += order.quantity 
                customer_quantities[order.ID] = order.quantity

        aggregate_orders.append(AggregateOrder(
            customer_quantities= customer_quantities, 
            firm_quantity= 100 - running_qty,
            total_quantity=100,
            current_price=current_price,
            current_time=current_time
        ))

    return aggregate_orders

def check_pending_orders(pending_orders, quote_price, quote_time, queue, ineligible_orders):
    if pending_orders:
        for p in pending_orders:
            if quote_time >= p.cutoff_time:
                heapq.heappush(queue, (p.cutoff_time, p, quote_price, quote_time))
        pending_orders.clear()

def execute(queue, aggregate_orders, ineligible_orders, pending_orders):
    running_quantity = 0
    while queue:
        cutoff_time, order, quote_price, quote_time = heapq.heappop(queue)
        if order.quantity >= 100:
            order.quantity -= 100
            if order_is_eligible(quote_price, quote_time, order):
                aggregate_orders.append(AggregateOrder(
                    customer_quantities= {order.ID :  100}, 
                    total_quantity=100,
                    current_price=quote_price,
                    current_time=order.current_time
                ))
                if order.quantity > 0:
                    heapq.heappush(queue, (cutoff_time, order, quote_price, quote_time))
            else:
                if abs(quote_price - order.target_price) > 10:
                    ineligible_orders[order.ID] = order
                    continue
                heapq.heappush(queue, (cutoff_time, order, quote_price, quote_time))
        else:
            if order.cutoff_time >= quote_time:
                if order_is_eligible(quote_price, quote_time, order):
                    running_quantity += order.quantity
                    pending_orders.append(order)
                else:
                    ineligible_orders[order.ID] = order
                    continue

                if running_quantity == 100:
                    customer_quantities = {}
                    for po in pending_orders:
                        customer_quantities[po.ID] = po.quantity
                    aggregate_orders.append(AggregateOrder(
                        customer_quantities= customer_quantities, 
                        total_quantity=100,
                        current_price=quote_price,
                        current_time=order.current_time
                    ))
                    running_quantity = 0
                    pending_orders.clear()
                elif running_quantity > 100:
                    total_quantity = 0
                    customer_quantities = {}
                    for p in pending_orders:
                        total_quantity += p.quantity
                        if total_quantity > 100:
                            delta = total_quantity - 100
                            customer_quantities[p.ID] = p.quantity - delta
                            p.quantity = delta
                            heapq.heappush(queue, (p.cutoff_time, p, quote_price, quote_time))
                            aggregate_orders.append(AggregateOrder(
                                customer_quantities= customer_quantities, 
                                total_quantity=100,
                                current_price=quote_price,
                                current_time=order.current_time
                            ))
                            running_quantity = 0
                            pending_orders.clear()
                        else:
                            customer_quantities[p.ID] = p.quantity
            else:
                aggregate_orders.append(AggregateOrder(
                    customer_quantities= {order.ID :  order.quantity}, 
                    total_quantity=100,
                    firm_quantity= 100 - order.quantity,
                    current_price=quote_price,
                    current_time=order.current_time
                ))
                pending_orders.clear()
    
    for i in pending_orders:
        if i.cutoff_time >= quote_time:
            aggregate_orders.append(AggregateOrder(
                customer_quantities= {i.ID :  i.quantity}, 
                total_quantity=100,
                firm_quantity= 100 - i.quantity,
                current_price=quote_price,
                current_time=i.current_time
            ))
        
    pending_orders.clear()

def order_is_eligible(quote_price, quote_time, order):
    return quote_time <= order.cutoff_time and abs(quote_price - order.target_price) < 10

def main():
    messages = [
        Quote(current_price=10, current_time=1),
        Order(ID="0", target_price=10, quantity=100, current_time=2, cutoff_time=10),
        Order(ID="1", target_price=10, quantity=300, current_time=3, cutoff_time=10),
        Quote(current_price=12, current_time=4),
        Order(ID="2", target_price=10, quantity=20, current_time=4, cutoff_time=10),
        Order(ID="3", target_price=10, quantity=30, current_time=5, cutoff_time=10),
        Order(ID="4", target_price=10, quantity=50, current_time=6, cutoff_time=10),
        Order(ID="5", target_price=10, quantity=130, current_time=7, cutoff_time=10),
        Order(ID="6", target_price=10, quantity=80, current_time=10, cutoff_time=10),
        Order(ID="7", target_price=23, quantity=10, current_time=11, cutoff_time=11),
        Order(ID="8", target_price=1, quantity=20, current_time=11, cutoff_time=11),
        Order(ID="9", target_price=9, quantity=70, current_time=11, cutoff_time=11),
        Order(ID="10", target_price=30, quantity=50, current_time=12, cutoff_time=20),
        Quote(current_price=33, current_time=12),
    ]

    expected_orders = [
        AggregateOrder(customer_quantities={"0": 100}, firm_quantity=0, total_quantity=100, current_price=10, current_time=2),
        AggregateOrder(customer_quantities={"1": 100}, firm_quantity=0, total_quantity=100, current_price=10, current_time=3),
        AggregateOrder(customer_quantities={"1": 100}, firm_quantity=0, total_quantity=100, current_price=10, current_time=3),
        AggregateOrder(customer_quantities={"1": 100}, firm_quantity=0, total_quantity=100, current_price=10, current_time=3),
        AggregateOrder(customer_quantities={"2": 20, "3": 30, "4": 50}, firm_quantity=0, total_quantity=100, current_price=12, current_time=6),
        AggregateOrder(customer_quantities={"5": 100}, firm_quantity=0, total_quantity=100, current_price=12, current_time=7),
        AggregateOrder(customer_quantities={"5": 30, "6": 70}, firm_quantity=0, total_quantity=100, current_price=12, current_time=10),
        AggregateOrder(customer_quantities={"6": 10}, firm_quantity=90, total_quantity=100, current_price=12, current_time=10),
        AggregateOrder(customer_quantities={"9": 70}, firm_quantity=30, total_quantity=100, current_price=12, current_time=11),
        AggregateOrder(customer_quantities={"7": 10, "10": 50}, firm_quantity=40, total_quantity=100, current_price=33, current_time=12),
    ]

    orders = aggregate_orders_solution(messages)
    min_num_orders = min(len(orders), len(expected_orders))
    for i in range(min_num_orders):
        if orders_match(orders[i], expected_orders[i]):
            print(f"Order {i}: ok")
        else:
            print(f"Order {i}: expected {expected_orders[i]}, got {orders[i]}")
            print("TEST FAILED")
            return

    if len(orders) > len(expected_orders):
        print("Extra orders:")
        for order in orders[min_num_orders:]:
            print(order)
        print("TEST FAILED")
    elif len(orders) < len(expected_orders):
        print("Missing orders:")
        for order in expected_orders[min_num_orders:]:
            print(order)
        print("TEST FAILED")

def orders_match(order1, order2):
    if order1.firm_quantity != order2.firm_quantity or \
       order1.current_price != order2.current_price or \
       order1.current_time != order2.current_time or \
       order1.total_quantity != order2.total_quantity:
        return False
    if len(order1.customer_quantities) != len(order2.customer_quantities):
        return False
    for order_id, quantity in order1.customer_quantities.items():
        if order2.customer_quantities.get(order_id) != quantity:
            return False
    return True

if __name__ == "__main__":
    main()
