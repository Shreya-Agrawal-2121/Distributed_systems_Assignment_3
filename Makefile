build:
	sudo docker-compose build
run_mysql:
	sudo docker-compose up -d mysql
run_lb:
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
