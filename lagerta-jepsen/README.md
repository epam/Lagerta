# Testing Lagerta with Jepsen

## Getting Started
All tests placed in ./src, on docker container placed in /lagerta/src
written on [clojure](https://clojure.org/)

### Prerequisites
Requires any os with bash support and installed docker.
Ubuntu has some issues with docker networking,
see on [github](https://github.com/moby/moby/issues/1809).

In development we use clojure plugin for IDE and Leiningen.

### Installing
move into docker folder and 
run `build.sh` script, then
```
docker-compose up -d
```
**to stop all containers:**
```
docker kill $(docker ps -q)
```
or
```
docker-compose kill
```
**to scale kafka nodes**
```
docker-compose scale scalable-kafka=3
```
see for more info https://github.com/wurstmeister/kafka-docker

## Running the tests
**to enter to jepsen-control container:**
```
docker exec -it jepsen-control bash
```
**to run a check tests:**
```
docker exec jepsen-control bash -c "cd /jepsen-0.1.4/jepsen && lein test"
```
**to run a lagerta tests:**
```
docker exec jepsen-control bash -c "cd /lagerta && lein run test"
```