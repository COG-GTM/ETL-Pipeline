# Deriving the python image
FROM python:3.11-slim

# Create a working directory in Docker, makes life easier when running instructions
WORKDIR /app

# Installs all the libraries we will need to execute the code
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copies all the source code into our directory to the Docker image
COPY . /app

# Tell Docker the command to run inside the container
CMD ["python", "./main.py"]