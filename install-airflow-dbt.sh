#!/bin/bash

# Docker Installation

# 1. Remove conflicting packages
sudo apt remove $(dpkg --get-selections docker.io docker-compose docker-compose-v2 docker-doc podman-docker containerd runc | cut -f1)

# 2. Add Docker's official GPG key
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# 3. Add the repository to Apt sources

sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

# 4. Installing docker
# 여기서의 apt 업데이트는 새로 추가한 docker repository 정보를 반영하기 위함
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

# Installing Airflow with DBT

# 1. Create project directory
# mkdir CatchData-airflow
cd catchdata-airflow

# 2. Download Airflow docker-compose.yaml
# curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.2/docker-compose.yaml'

# 3. Create directories
# mkdir -p ./dags ./logs ./plugins ./config ./dbt
mkdir -p ./logs ./plugins ./config

# 4. Create .env file
echo "AIRFLOW_UID=$(id -u)" > .env

# 5. Create Dockerfile with DBT
# 5-1. postgresql 의존성 문제로 psycopg2-binary를 먼저 설치
cat > Dockerfile <<'EOF'
FROM apache/airflow:3.1.2
USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    dbt-core \
    dbt-postgres \
    dbt-redshift
EOF

# 6. Modify docker-compose.yaml to use custom build
sed -i 's|image: \${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.2}|build: .|g' docker-compose.yaml

# 6-1. Build and start Airflow
sudo docker compose build --no-cache
sudo docker compose up airflow-init

# 7. Add docker group and initialize
sudo usermod -aG docker $USER
echo "Setup complete! Run: docker compose up airflow-init && docker compose up -d"
