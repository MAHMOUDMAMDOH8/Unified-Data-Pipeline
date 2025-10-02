import json
import random
import time
from datetime import datetime, timedelta

class LogGenerator:
    def __init__(self):
        # Sample data stores
        self.users = [f"user_{i}" for i in range(1, 1001)]
        self.categories = [
            "Beverages",
            "Condiments",
            "Confections",
            "Dairy Products",
            "Grains/Cereals",
            "Meat/Poultry",
            "Produce",
            "Seafood",
        ]
        self.products = [
            {
                "product_id": i,  # 1..77 numeric IDs per requirement
                "name": f"Product {i}",
                "category": random.choice(self.categories),
                "price": round(random.uniform(10, 500), 2),  # strictly > 0
            }
            for i in range(1, 78)
        ]
        self.device_types = [
            "desktop", "mobile", "tablet"
        ]
        self.geo_locations = [
            {"country": "Egypt", "city": random.choice(["Cairo", "Alexandria", "Giza", "Mansoura", "Asyut"])},
            {"country": "Saudi Arabia", "city": random.choice(["Riyadh", "Jeddah", "Mecca", "Dammam", "Medina"])},
            {"country": "United Arab Emirates", "city": random.choice(["Dubai", "Abu Dhabi", "Sharjah"])},
            {"country": "Qatar", "city": random.choice(["Doha"])},
            {"country": "Kuwait", "city": random.choice(["Kuwait City", "Hawalli"])},
            {"country": "Oman", "city": random.choice(["Muscat", "Salalah"])},
            {"country": "Jordan", "city": random.choice(["Amman", "Zarqa"])},
            {"country": "Lebanon", "city": random.choice(["Beirut", "Tripoli"])},
            {"country": "Iraq", "city": random.choice(["Baghdad", "Erbil", "Basra"])},
            {"country": "Palestine", "city": random.choice(["Gaza", "Nablus", "Ramallah"])},
        ]

    def random_device_type(self):
        return random.choice(self.device_types)

    def random_geo_location(self):
        return random.choice(self.geo_locations)

    def random_session_duration(self):
        return random.randint(300, 1800)  # 5 to 30 minutes, always > 0

    def _generate_nonempty_address(self, city: str, country: str) -> str:
        street_num = random.randint(100, 999)
        street_name = random.choice(["Main St", "Market St", "High St", "Elm St", "Oak Ave"])  # always present
        return f"{street_num} {street_name}, {city}, {country}"

    def _pick_product_items(self):
        # 1-3 items, ensure positive qty and amount
        num_items = random.randint(1, 3)
        items = []
        total = 0.0
        for _ in range(num_items):
            product = random.choice(self.products)
            quantity = random.randint(1, 4)
            line_amount = round(product["price"] * quantity, 2)
            total += line_amount
            items.append({
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "line_amount": line_amount,
                "category": product["category"],
            })
        return items, round(total, 2)

    def generate_log(self):
        """Generate a single completed order event (event_type='order') with no nulls/zeros."""
        user_id = random.choice(self.users)
        geo = self.random_geo_location()
        items, total = self._pick_product_items()

        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": "INFO",
            "service": "online-store",
            "event_type": "order",
            "user_id": user_id,
            "session_id": f"sess_{random.randint(10000, 99999)}",
            "session_duration": self.random_session_duration(),
            "device_type": self.random_device_type(),
            "geo_location": {"country": geo["country"], "city": geo["city"]},
            "details": {
                "order_id": f"order_{random.randint(100000, 999999)}",  # string
                "status": "completed",
                "items": items,
                "order_amount": total,
                "currency": "USD",
                "shipping_method": random.choice(["standard", "express", "pickup"]),
                "shipping_address": self._generate_nonempty_address(geo['city'], geo['country']),
                "warehouse": random.choice(["WH-Cairo", "WH-Riyadh", "WH-Dubai","WH-Kuwait"]),
                "carrier": random.choice(["DHL", "Aramex", "FedEx"]),
                "tracking_number": f"TRK{random.randint(10000000,99999999)}",
                "completed_at": (datetime.utcnow() - timedelta(seconds=random.randint(30, 180))).isoformat() + "Z",
                "delivery_estimate": (datetime.utcnow() + timedelta(days=random.randint(1, 7))).isoformat() + "Z"
            }
        }

        return log_entry

    def generate_logs(self, count=10, interval=0.5):
        """Generate multiple logs with time intervals"""
        logs = []
        for _ in range(count):
            log = self.generate_log()
            logs.append(log)
            time.sleep(interval)
        return logs

if __name__ == "__main__":
    generator = LogGenerator()
    
    # Generate 10 sample logs with 0.5s interval
    print("=== Generating Sample Online Store Order Logs (Completed) ===")
    generator.generate_logs(count=10, interval=0.5)