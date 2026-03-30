# CatchData-Airflow - 데이터 파이프라인

## 목차

- [프로젝트 소개](#-프로젝트-소개)
- [주요 기능](#-주요-기능)
- [기술 스택](#-기술-스택)
- [시스템 아키텍처](#-시스템-아키텍처)
- [설치 및 실행](#-설치-및-실행)
- [프로젝트 구조](#-프로젝트-구조)

## 프로젝트 소개

CatchData-Airflow는 **스마트 맛집 추천 플랫폼**을 위한 데이터 파이프라인. Apache Airflow를 사용하여 카카오맵 크롤링부터 데이터 웨어하우스 구축, 실시간 추천 스코어 계산까지 전체 데이터 흐름을 자동화

### 핵심 가치

- **자동화된 데이터 수집**: 카카오맵 API와 Selenium을 활용한 일일 맛집 데이터 크롤링
- **확장 가능한 데이터 웨어하우스**: AWS Redshift 기반 분석 플랫폼
- **실시간 추천 시스템**: 시간대별 방문 패턴과 대기 정보를 반영한 동적 추천 스코어
- **안정적인 운영**: Slack 알림 기반 DAG 모니터링 및 장애 감지

## 주요 기능

### 1. 카카오맵 크롤링 (ver2_01)
- **카카오맵 REST API**를 통한 맛집 기본 정보 수집
- **Selenium 기반 웹 크롤링**으로 실시간 대기 정보 획득
- OCR(Tesseract)을 활용한 대기 인원 이미지 파싱
- 멀티프로세싱으로 크롤링 성능 최적화
- **AWS S3** 자동 업로드 (CSV 형식)

**수집 데이터:**
- 식당명, 주소, 전화번호, 카테고리, 평점
- 좌표 (위도/경도)

### 2. S3 → Redshift 데이터 적재
- S3에서 Redshift로 일일 크롤링 데이터 자동 로드
- `COPY` 명령 사용으로 대용량 데이터 효율적 처리
- 중복 제거 및 데이터 정합성 검증

### 3. 정적 피처 엔지니어링
- **시간대별 방문 패턴 분석** (time0~time23)
- **품질 점수(quality_score)** 계산
  - 평점, 리뷰 수, 카테고리 가중치 반영
- **클러스터링 기반 유사 맛집 그룹핑**
- 정규화된 base population 계산

**생성 테이블:**
```sql
analytics.derived_features_base (
    id, base_population, quality_score,
    rating, time0~time23, cluster, ...
)
```

### 4. 실시간 추천 스코어 계산
- **3가지 추천 전략** 구현
  - `rec_quality`: 품질 중심 (평점/리뷰 60%)
  - `rec_balanced`: 균형 중심 (품질 40% + 대기 35%)
  - `rec_convenience`: 편의성 중심 (대기시간 50%)
- 현재 시간대 방문 패턴 반영
- 실시간 대기 인원 기반 동적 계산

**추천 로직:**
```
rec_quality = 0.6×quality + 0.2×rating + 0.2×(1-wait_norm)
rec_balanced = 0.4×quality + 0.35×rating + 0.25×(1-wait_norm)
rec_convenience = 0.3×quality + 0.2×rating + 0.5×(1-wait_norm)
```

### 5. 맵 검색 데이터 수집
- 사용자 맵 검색 이력 추적
- 지역/도시/카테고리별 검색 통계
- 대시보드 필터링 데이터 소스

### 6. DAG 모니터링 (monitor_dag_status)
- **실패한 DAG/Task 자동 감지**
- 30분 이상 running 상태인 DAG 알림
- **Slack 웹훅**을 통한 실시간 알림

**알림 시나리오:**
```
Airflow DAG 모니터링 알림
실패한 DAG: kakao_crawl_v2 (2025-12-26 14:30)
실패한 Task: load_to_redshift (2025-12-26 14:35)
장시간 실행 DAG: feature_engineering (35분 경과)
```

### 7. DBT 분석 파이프라인 (dbt_analytics_daily)
- DBT 모델 자동 실행
- 일일 분석 지표 계산
- 데이터 품질 검증

## 기술 스택

### Workflow Orchestration
- **Apache Airflow 3.1.2** - 워크플로우 관리
- **CeleryExecutor** - 분산 작업 처리
- **Redis** - 메시지 브로커
- **PostgreSQL** - 메타데이터 저장소

### Data Collection
- **Selenium 4.10.0** - 웹 크롤링
- **Tesseract OCR** - 이미지 텍스트 인식
- **OpenCV** - 이미지 전처리
- **Kakao REST API** - 맛집 정보 조회

### Data Warehouse
- **Amazon Redshift** - 분석용 데이터베이스
- **Amazon S3** - 데이터 레이크
- **DBT** - 데이터 변환

### DevOps
- **Docker Compose** - 컨테이너 오케스트레이션
- **GitHub Actions** - CI/CD
  - Ruff 코드 품질 검사
  - EC2 자동 배포 (Bastion Host 경유)
- **AWS EC2** - Airflow 서버 호스팅

### Monitoring
- **Slack Webhooks** - 알림
- **Airflow UI** - DAG 모니터링

## 시스템 아키텍처

### 데이터 파이프라인 흐름

```
### Airflow 아키텍처
```
<img width="2000" height="1500" alt="데이터flowdiagram drawio의 사본 drawio (1)" src="https://github.com/user-attachments/assets/d9db8189-bf0c-48be-b99e-61ea72f4a952" />


### AWS 아키텍쳐
<img width="2000" height="1600" alt="aws아키텍쳐 drawio의 사본 drawio (2)" src="https://github.com/user-attachments/assets/233227a9-c50d-4a46-a27f-7d25e603612b" />



## 설치 및 실행

### 사전 요구사항
- Docker & Docker Compose
- AWS 계정 (S3, Redshift, EC2)
- Kakao Developers API Key
- Slack Webhook URL (선택)

### 1. 저장소 클론
```bash
git clone https://github.com/Team5-CatchData/CatchData-Airflow.git
cd CatchData-Airflow/catchdata-airflow
```

### 2. 환경변수 설정
`.env` 파일을 생성하고 다음 내용을 설정:
```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=

# AWS Credentials
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=

# S3
S3_BUCKET_NAME=

# Redshift
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DB=
REDSHIFT_USER=
REDSHIFT_PASSWORD=

# API Keys (Airflow Variables)
KAKAO_REST_API_KEY=
SLACK_WEBHOOK_URL=
```

### 3. Docker 이미지 빌드

install-airflow-dbt.md 참고

### 4. Airflow Connection 설정
Airflow UI → Admin → Connections에서 redshift와 RDS 커넥션 추가

### 5. Airflow Variables 설정
Admin → Variables에서 추가:
```json
{
  "KAKAO_REST_API_KEY": "your_api_key",
  "SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/...",
  "S3_BUCKET_NAME": "your-bucket-name"
}
```

### 8. DAG 실행
- Webserver UI에서 DAG 활성화
- 수동 실행: DAG 선택 → Trigger DAG

### 9. 서비스 중지
```bash
docker compose down
```

### 10. 로그 확인
```bash
# 전체 로그
docker compose logs -f

# 특정 서비스 로그
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

## 프로젝트 구조

```
CatchData-Airflow/
├── .github/
│   └── workflows/
│       ├── ruffcheck.yml        # Ruff 린팅 (push/PR 시)
│       └── CD.yml               # EC2 자동 배포 (main 브랜치)
│
├── catchdata-airflow/
│   ├── dags/                    # Airflow DAG 파일
│   │   ├── ver2_01_kakao_crawl_all_in_one.py
│   │   ├── ver2_02_load_s3_to_redshift.py
│   │   ├── ver2_03_redshift_static_feature_update.py
│   │   ├── ver2_04_calculate_realtime_scores.py
│   │   ├── ver2_05_map_search.py
│   │   ├── monitor_dag_status.py
│   │   └── dbt_analytics_daily.py
│   │
│   ├── dbt/                     # DBT 프로젝트
│   │   └── common/
│   │       └── analyses/        # SQL 분석 쿼리
│   │
│   ├── config/
│   │   └── airflow.cfg          # Airflow 설정
│   │
│   ├── logs/                    # 실행 로그
│   ├── plugins/                 # 커스텀 플러그인
│   │
│   ├── docker-compose.yaml      # Docker 서비스 정의
│   ├── Dockerfile               # 커스텀 Airflow 이미지
│   ├── requirements.txt         # Python 의존성
│   └── .env                     # 환경변수 (git ignore)
│
├── install-airflow-dbt.sh       # 설치 스크립트
├── install-airflow-dbt.md       # 설치 가이드
└── README.md                    # 프로젝트 문서
```

## 성능 최적화

### 크롤링 최적화
- **멀티프로세싱**: CPU 코어 수 기반 병렬 처리
- **ChromeDriver Lock**: 동시 다운로드 방지 (threading.Lock)
- **헤드리스 모드**: GUI 없이 크롤링 하기 위함 + GPU 렌더링으로 인한 메모리 낭비 X

### Redshift 최적화
- **DISTKEY**: `id` 컬럼으로 분산 (JOIN 성능 향상)
- **SORTKEY**: `calculation_timestamp` 기준 정렬 (시계열 쿼리 최적화)
- **COPY 명령**: S3에서 대용량 데이터 고속 로드

### Airflow 최적화
- **CeleryExecutor**: Worker 수평 확장 가능
- **Task 병렬화**: 독립적인 Task는 동시 실행
- **Connection Pool**: DB 연결 재사용

## 모니터링 대시보드

### Airflow UI 주요 화면
1. **DAGs 뷰**: 전체 DAG 상태 한눈에 확인
2. **Graph 뷰**: Task 간 의존성 시각화
3. **Logs**: 각 Task 실행 로그 확인

## 기여 가이드

### 코드 스타일
- **Ruff** 린터 규칙 준수
- 체크 항목: `F,E,W,I,B,S` (보안, 품질, 가독성)
- 예외: `E501` (라인 길이), `S608` (SQL 인젝션 경고)

### Pull Request 프로세스
1. Feature 브랜치 생성
2. Ruff 검사 통과 확인
3. PR 생성 (main 브랜치로)
4. 리뷰 후 merge → 자동 배포

### 로컬 Ruff 검사
```bash
# 설치
pip install ruff

# 검사
ruff check . --select F,E,W,I,B,S --ignore E501,S608,S110

# 자동 수정
ruff check . --fix
```

## 참고 자료

- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Selenium Python 가이드](https://selenium-python.readthedocs.io/)
- [DBT 문서](https://docs.getdbt.com/)
