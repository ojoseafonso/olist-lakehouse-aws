#!/bin/bash
set -e

# 1. Atualizar pacotes
sudo dnf -y --releasever=latest update

# 2. Instalar Docker
sudo dnf install -y docker

# 3. Iniciar e habilitar Docker
sudo systemctl enable docker
sudo systemctl start docker

# 4. Instalar Docker Compose
# Cria o diretório de plugins do Docker
mkdir -p ~/.docker/cli-plugins

# Baixa a versão estável mais recente do Docker Compose
# O comando abaixo detecta automaticamente a arquitetura (x86_64 ou arm64)
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m) -o ~/.docker/cli-plugins/docker-compose

# Torna o arquivo executável
chmod +x ~/.docker/cli-plugins/docker-compose
sudo usermod -aG docker ec2-user

# Verifica a instalação
docker compose version

# 5. Criar diretórios do Airflow
# Set Airflow Home
export AIRFLOW_HOME=~/airflow

# Create required directories
mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Verify creation
ls -R ${AIRFLOW_HOME}

# 6. Configurar AIRFLOW_UID
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# 8. Clonar repo para docker-compose.yml
git clone https://github.com/ojoseafonso/olist-lakehouse-aws.git /home/ec2-user/app

# 8. Subir containers
docker-compose up -d 
