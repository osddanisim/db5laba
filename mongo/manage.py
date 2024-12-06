from pymongo import MongoClient
from pprint import pprint

# Підключення до MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["lab_database"]

# Створення колекцій та даних
db.users.insert_many([
    {"_id": 1, "name": "John", "surname": "Doe", "email": "john@example.com", "phone_number": "123456789"},
    {"_id": 2, "name": "Jane", "surname": "Smith", "email": "jane@example.com", "phone_number": "987654321"},
])

db.cars.insert_many([
    {"_id": 1, "model": "Civic", "brand": "Honda", "year": 2015, "license_plate": "ABC123", "user_id": 1},
    {"_id": 2, "model": "Corolla", "brand": "Toyota", "year": 2018, "license_plate": "XYZ789", "user_id": 2},
])

db.service_stations.insert_many([
    {"_id": 1, "station_name": "QuickFix", "address": "123 Main St", "contact_info": "111-222-3333"},
    {"_id": 2, "station_name": "AutoCare", "address": "456 Elm St", "contact_info": "444-555-6666"},
])

db.services.insert_many([
    {"_id": 1, "service_name": "Oil Change", "description": "Full oil change", "price": 29.99, "station_id": 1},
    {"_id": 2, "service_name": "Brake Inspection", "description": "Inspect and replace brakes", "price": 79.99, "station_id": 2},
])

db.orders.insert_many([
    {"_id": 1, "user_id": 1, "car_id": 1, "service_id": 1, "order_date": "2024-11-01T10:00:00", "order_status": "Completed"},
    {"_id": 2, "user_id": 2, "car_id": 2, "service_id": 2, "order_date": "2024-11-02T14:30:00", "order_status": "Pending"},
])

# Запит 1: З'єднання користувачів з їхніми замовленнями
pipeline1 = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "_id",
            "foreignField": "user_id",
            "as": "user_orders"
        }
    }
]

print("Запит 1: Користувачі та їх замовлення")
pprint(list(db.users.aggregate(pipeline1)))

# Запит 2: З'єднання замовлень з інформацією про автомобілі та послуги
pipeline2 = [
    {
        "$lookup": {
            "from": "cars",
            "localField": "car_id",
            "foreignField": "_id",
            "as": "car_details"
        }
    },
    {
        "$lookup": {
            "from": "services",
            "localField": "service_id",
            "foreignField": "_id",
            "as": "service_details"
        }
    }
]

print("\nЗапит 2: Замовлення з деталями автомобілів і послуг")
pprint(list(db.orders.aggregate(pipeline2)))

# Запит 3: З'єднання послуг з відповідними станціями обслуговування
pipeline3 = [
    {
        "$lookup": {
            "from": "service_stations",
            "localField": "station_id",
            "foreignField": "_id",
            "as": "station_details"
        }
    }
]

print("\nЗапит 3: Послуги та їх станції обслуговування")
pprint(list(db.services.aggregate(pipeline3)))

# Запит 4: Фільтр замовлень за статусом
pipeline4 = [
    {"$match": {"order_status": "Pending"}}
]

print("\nЗапит 4: Замовлення зі статусом 'Pending'")
pprint(list(db.orders.aggregate(pipeline4)))

# Запит 5: Групування автомобілів за брендом
pipeline5 = [
    {
        "$group": {
            "_id": "$brand",
            "total_cars": {"$sum": 1}
        }
    }
]

print("\nЗапит 5: Групування автомобілів за брендом")
pprint(list(db.cars.aggregate(pipeline5)))
