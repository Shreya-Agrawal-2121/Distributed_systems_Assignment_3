build:
	sudo docker-compose build

run_lb:
	sudo docker-compose up lb

run_servers:
	curl -X POST -H "Content-Type: application/json" -d '{"n": 3, "hostnames": ["s1", "s2", "s3"]}' http://localhost:5000/add

stop:
	for container in $$(sudo docker ps -q); do sudo docker stop $$container; done
	sudo docker system prune -f

rm:
	for image in $$(sudo docker images -q); do sudo docker rmi $$image; done
	sudo docker system prune -f
