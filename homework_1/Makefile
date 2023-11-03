.PHONY : setup teardown clean app minio pulsar

setup: minio app pulsar

teardown:
	docker stop mockapi-container minio-container pulsar-container && docker rm mockapi-container minio-container pulsar-container

app: build-app run-app

build-app: # build a container for the fastapi API
	cd api && cp ../.env . && docker build -t mockapi . && rm .env

run-app: # run the fastapi API in container
	docker run -d --name mockapi-container -p 80:80 mockapi

minio: # run a local minio instance
	docker run -d -p 9000:9000 -p 9090:9090 --name minio-container -v ~/minio/data:/data -e "MINIO_ROOT_USER=admin" -e "MINIO_ROOT_PASSWORD=adminadmin" quay.io/minio/minio server /data --console-address ":9090"

pulsar: # run a local pulsar instance
	docker run -d -p 6650:6650 -p 8081:8080 --name pulsar-container apachepulsar/pulsar:3.1.1 bin/pulsar standalone

clean: # recursively clean python cache files
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete
