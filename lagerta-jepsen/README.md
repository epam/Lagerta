start jepsen environment

```
sh build.sh
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

All tests placed in src/test and witten on [clojure](https://clojure.org/)