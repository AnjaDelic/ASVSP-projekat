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

