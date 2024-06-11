docker exec -it mongodb bash
mongosh
use accidents
db.createCollection("accidents_data")
use admin
db.createUser(
  {
    user: "metabase",
    pwd: "metabase",
    roles: [ { role: "readWrite", db: "accidents" } ]
  }
)
db.getUsers()

#Name: Accidents DB
#Host: mongodb
#Port: 27017
#Database Name: accidents
#Username: metabase
#Password: metabase
#Authentication Database Name: admin
