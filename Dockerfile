# Deriving the python image
FROM python:3.12-slim

RUN . /etc/os-release \
    && test "$VERSION_CODENAME" = trixie \
    && apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade -t trixie-security zlib1g \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' zlib1g)" ge '1:1.3.dfsg+really1.3.1-1+deb13u1' \
    && rm -rf /var/lib/apt/lists/*

# Create a working directory in Docker, makes life easier when running instructions
WORKDIR /app

# Copies all the source code into our directory to the Docker image
COPY . /app

# Installs all the libraries we will need to execute the code
RUN pip install -r requirements.txt

# Tell Docker the command to run inside the container
CMD ["python", "./main.py"]