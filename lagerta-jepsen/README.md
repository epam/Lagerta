### start jepsen environment

move into docker folder and 
run `build.sh` script, then
```
docker-compose up -d
```
**stop all containers:**
```
docker kill $(docker ps -q)
```
or
```
docker-compose kill
```
**enter to jepsen-control container:**
```
docker exec -it jepsen-control bash
```
**submit a check test:**
```
docker exec jepsen-control bin/bash -c "cd /jepsen-0.1.4/jepsen && lein test"
```

All tests placed in ./src, on docker container placed in /lagerta/src

written on [clojure](https://clojure.org/)
