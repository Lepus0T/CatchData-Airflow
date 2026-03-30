# Airflow & DBT 설치 및 배포 가이드

이 문서는 EC2 인스턴스에 Docker를 사용하여 Airflow와 DBT를 설치하고, GitHub Actions를 통해 자동 배포하는 방법을 설명합니다.

## 목차

1. [설치 스크립트 제작](#1-설치-스크립트-제작)
2. [GitHub Actions 워크플로우 설정](#2-github-actions-워크플로우-설정)
3. [EC2 인스턴스 설정](#3-ec2-인스턴스-설정)
4. [설치 실행](#4-설치-실행)
5. [트러블슈팅](#5-트러블슈팅)

---

## 1. 설치 스크립트 제작

### 1.1 설치 스크립트 구조

`install-airflow-dbt.sh` 파일을 프로젝트 루트에 생성합니다.

### 1.2 스크립트 파일 생성 위치

```
port-airflow/
├── install-airflow-dbt.sh          # 설치 스크립트
├── docker-compose.yaml             # Docker Compose 설정
├── Dockerfile                      # Airflow + DBT 이미지
└── catchdata-airflow/
    ├── dags/
    ├── plugins/
    └── config/
```

---

## 2. GitHub Actions 워크플로우 설정

### 2.1 디렉토리 구조

```
.github/
└── workflows/
    ├── cd.yml          # 자동 배포
    └── ruff-check.yml  # 코드 품질 검사
```

### 2.2 CD 워크플로우 (cd.yml)

`.github/workflows/cd.yml` 파일 생성:


### 2.3 Ruff 검사 워크플로우 (ruff-check.yml)

`.github/workflows/ruff-check.yml` 파일 생성:

### 2.4 GitHub Secrets 설정

GitHub 저장소 Settings → Secrets and variables → Actions에서 다음 Secrets 추가:
---


## 3. 설치 실행

### 3.1 선행 작업

```bash
git clone https://github.com/zhfem05-star/CatchData-Airflow
```

### 3.2 설치 스크립트 권한 설정

```bash
chmod +x install-airflow-dbt.sh
```

### 3.3 설치 스크립트 실행

```bash
./install-airflow-dbt.sh
```

설치 과정:
1. ✅ 시스템 패키지 업데이트
2. ✅ Docker Repository 추가
3. ✅ Docker 설치
4. ✅ 작업 디렉토리 생성
5. ✅ 디렉토리 구조 생성
6. ✅ Docker 이미지 빌드
7. ✅ Airflow 초기화

### 3.4 서비스 시작

```bash
sudo docker compose up -d
```

### 3.5 서비스 상태 확인

```bash
# 컨테이너 상태 확인
sudo docker compose ps

# 로그 확인
sudo docker compose logs -f
```

## 5. 트러블슈팅

### 5.1 Docker 권한 오류

**증상**: `permission denied while trying to connect to the Docker daemon socket`

**해결**:
```bash
sudo usermod -aG docker $USER
newgrp docker
# 또는 재로그인
```

### 5.2 포트 충돌

**증상**: `port is already allocated`

**해결**:
```bash
# 사용 중인 포트 확인
sudo lsof -i :8080

# 해당 프로세스 종료
sudo kill -9 <PID>
```

### 5.3 Airflow 초기화 실패

**증상**: `airflow-init` 컨테이너가 실패

**해결**:
```bash
# 기존 컨테이너 및 볼륨 삭제
sudo docker compose down -v

# 다시 초기화
sudo docker compose up airflow-init
sudo docker compose up -d
```

### 5.4 GitHub Actions 배포 실패

**증상**: SSH 연결 실패

**해결**:
1. GitHub Secrets에 올바른 SSH 키가 설정되어 있는지 확인
2. EC2 보안 그룹에서 22번 포트가 열려있는지 확인
3. EC2 인스턴스가 실행 중인지 확인

### 5.5 메모리 부족

**증상**: 컨테이너가 자주 재시작됨

**해결**:
```bash
# 불필요한 이미지 및 컨테이너 정리
sudo docker system prune -a

# EC2 인스턴스 타입 업그레이드 (t2.medium → t2.large)
```

---

## 6. 유용한 명령어

### Docker Compose 명령어

```bash
# 서비스 시작
sudo docker compose up -d

# 서비스 중지
sudo docker compose down

# 서비스 재시작
sudo docker compose restart

# 로그 확인
sudo docker compose logs -f

# 특정 서비스 로그만 확인
sudo docker compose logs -f airflow-webserver

# 컨테이너 상태 확인
sudo docker compose ps

# 컨테이너 내부 접속
sudo docker compose exec airflow-webserver bash
```

### Airflow CLI 명령어

```bash
# DAG 목록 확인
sudo docker compose exec airflow-webserver airflow dags list

# DAG 테스트
sudo docker compose exec airflow-webserver airflow dags test <dag_id> <execution_date>

# Task 테스트
sudo docker compose exec airflow-webserver airflow tasks test <dag_id> <task_id> <execution_date>

# Variable 설정
sudo docker compose exec airflow-webserver airflow variables set <key> <value>

# Connection 확인
sudo docker compose exec airflow-webserver airflow connections list
```

---

## 7. 다음 단계

설치가 완료되면:

1. ✅ Airflow UI에 접속하여 DAG 확인
2. ✅ Slack 알림 설정 ([plugin/README.md](catchdata-airflow/plugin/README.md) 참고)
3. ✅ Airflow Variables 설정
4. ✅ Airflow Connections 설정 (AWS, Snowflake 등)
5. ✅ DAG 활성화 및 테스트

---

## 참고 자료

- [이전 프로젝트 문서](https://github.com/DE7-SamRa/samra-airflow)
- [Airflow 공식 문서](https://airflow.apache.org/docs/)
- [DBT 공식 문서](https://docs.getdbt.com/)
- [Docker Compose 문서](https://docs.docker.com/compose/)
- [GitHub Actions 문서](https://docs.github.com/en/actions)
