# flight-data-service

### Build it
Run `docker-compose build` to build an image for api and workers containers.

### Run it
Run `docker-compose up` to run all containers.

### Get datasets info
Run `curl http://127.0.0.1:5000/info/`

### Get aircraft models
Run `curl http://127.0.0.1:5000/aircrafts/models/`

### Get active aircraft by manufacturer and model
Run `curl http://127.0.0.1:5000/aircrafts/?manufacturer=manufacturer&model=model`

### Get report of active aircraft models by state
Run `curl http://127.0.0.1:5000/aircrafts/report/`

### Get pivot report of active aircraft models by count
Run `curl http://127.0.0.1:5000/aircrafts/report/pivot/`

### Get data by SQL string
Run `curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"sql":"<SQL string>"}' \
  http://127.0.0.1:5000/sql/`
