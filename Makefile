build:
	sudo docker-compose build
	docker network inspect n1 >/dev/null 2>&1 || docker network create --driver bridge n1
	sudo docker run --name mysql_db --hostname mysql_db --network n1 -e MYSQL_ROOT_PASSWORD=test -d mysql:latest
run_lb:
	sudo docker-compose up -d sm
	sudo docker-compose up lb

clean_containers:
	for container in $$(sudo docker ps -a -q); do sudo docker stop $$container; done
	for container in $$(sudo docker ps -a -q); do sudo docker rm $$container; done

stop:
	for container in $$(sudo docker ps -q); do sudo docker stop $$container; done
	sudo docker system prune -f

rm:
	for image in $$(sudo docker images -q); do sudo docker rmi $$image; done
	sudo docker system prune -f

# sudo docker rm $(sudo docker ps -aq)
# if stops doesnot works, ```sudo aa-remove-unknown```
# sudo docker stop $(sudo docker ps -aq)
