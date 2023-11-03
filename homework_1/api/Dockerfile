# Base image
FROM python:3.10

# Set the working directory
WORKDIR /code

ENV PYTHONPATH=/app

# Copy the requirements and environment file
COPY ./requirements.txt /code/requirements.txt

COPY ./.env /code/.env

# Install the requirements
RUN pip install -r /code/requirements.txt

# Copy the application to run folder
COPY ./app /code/app

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
