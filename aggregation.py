import os
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv()
dburl = os.getenv("DBURL")
#print(dburl)
#creating client
Client = MongoClient(dburl)
#creatingdatabase
database = Client['HousePrices']
#creating collections
collection = database.pricesInfo
#freshen up the collection to prevent duplication
collection.delete_many({})
#populating database
priceInfo = [
    {"location": "Kathmandu", "price": 120000, "noOfRooms": 3, "noOfFloors": 2},
    {"location": "Lalitpur", "price": 150000, "noOfRooms": 4, "noOfFloors": 3},
    {"location": "Bhaktapur", "price": 100000, "noOfRooms": 2, "noOfFloors": 1},
    {"location": "Kathmandu", "price": 130000, "noOfRooms": 3, "noOfFloors": 2},
    {"location": "Lalitpur", "price": 160000, "noOfRooms": 4, "noOfFloors": 4},
    {"location": "Pokhara", "price": 110000, "noOfRooms": 2, "noOfFloors": 2},
    {"location": "Pokhara", "price": 180000, "noOfRooms": 5, "noOfFloors": 3},
    {"location": "Biratnagar", "price": 95000, "noOfRooms": 2, "noOfFloors": 1},
    {"location": "Biratnagar", "price": 105000, "noOfRooms": 3, "noOfFloors": 2},
    {"location": "Kathmandu", "price": 170000, "noOfRooms": 5, "noOfFloors": 4}
]
result =  collection.insert_many(priceInfo)
#show ids if successfully inserted in mongodb database
#print("Inserted IDs:", result.inserted_ids) 
aggregatedresult=list(database.pricesInfo.aggregate([
    {"$match" : {"location":"Kathmandu"}},
    {"$group" : {
                "_id":"$_id", #group by _id i.e each document its own ground as id unique
                "price" : {
                            "$first":"$price"
                          },
                 "location" : {
                            "$first":"$location"
                          },
                "floorrooms" : {"$sum":{   #additional fields : {acc :expn}
                                         "$add":["$noOfFloors","$noOfRooms"]
                                        }
                                }
                }
    },
    {"$sort" : {"floorrooms" : -1 , "price": 1}},
]))

import json
mydict = []
with open("output.json","w") as f:
    for finalagg in aggregatedresult:
        mydict.append({
            "location": str(finalagg["location"]),
            "price": finalagg["price"],
            "floorrooms":finalagg["floorrooms"]
            })
    f.write(json.dumps(mydict,indent=4))

for finalagg in aggregatedresult:
    print(finalagg)






