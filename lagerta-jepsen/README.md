start jepsen environment

```
sh build.sh
docker-compose up -d
```
on windows
```
docker-compose up -v ./jepsen-control/init.sh:/init.sh
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
docker exec -it jepsen-control /bin/bash
```
**submit a check test:**
```
docker exec jepsen-control bin/bash -c "cd /jepsen-0.1.4/jepsen && lein test"
```