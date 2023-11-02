.PHONY : clean build-app run-app app

clean: # recursively clean python cache files
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete

app: build-app run-app

build-app:
	docker build -t mockapi .

run-app:
	docker run -d --name mockapi-container -p 80:80 mockapi
