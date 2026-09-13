# Agent Runtime Platform

에이전트 실행을 접수하고, 모델 판단과 도구 호출을 별도 worker에서 실행하며 결과를 PostgreSQL에 저장하는 Python 런타임입니다. 엔터프라이즈 운영을 목표로 단계적으로 구현합니다.

> 현재 상태: Phase 2b. lease·복구에 Tool effect 기록, idempotent mock provider, 취소·결과 조정 API를 추가했습니다. 실제 외부 provider의 exactly-once 보장이나 운영 배포 완료를 뜻하지 않습니다. 운영용 인증·실제 LLM/GPU 연동은 아직 없습니다.

## 로컬 실행

Python 3.13, uv, Docker가 필요합니다. 아래 명령은 이 저장소 루트에서 실행합니다. 예제 인증과 DB 비밀번호는 로컬 전용이며 API·DB를 외부에 공개하지 않습니다.

```bash
uv sync --frozen
cp .env.example .env
docker compose up -d --wait postgres
uv run --env-file .env alembic upgrade head
uv run python -m agent_platform.dev
uv run uvicorn agent_platform.api.app:create_app --factory --host 127.0.0.1 --port 8000
```

`agent_platform.dev`가 출력한 `agent_version_id`를 아래 요청에 사용합니다. 설치기는 loopback DB의 `runtime_local` 계정과 `demo` Tenant만 허용하며, 재실행 시 동일 버전이 일치하는지 확인합니다.

다른 터미널에서 같은 디렉터리의 worker를 실행합니다.

```bash
uv run python -m agent_platform.worker.main
```

```bash
curl -sS http://127.0.0.1:8000/v1/runs \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy' \
  -H 'Idempotency-Key: first-local-run' \
  -H 'Content-Type: application/json' \
  -d '{"agent_version_id":"<seed가 출력한 ID>","input":{"candidate_model_ref":"mock://candidate-v1","evaluation_suite_ref":"mock://suite-v1"}}'
```

`202` 응답의 `run_id`로 조회합니다. 동일 키·동일 입력의 재요청은 기존 Run을 반환하고, 같은 키의 다른 입력은 `409`입니다.

```bash
curl -sS http://127.0.0.1:8000/v1/runs/<run_id> \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy'
curl -N http://127.0.0.1:8000/v1/runs/<run_id>/events/stream \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy' \
  -H 'Last-Event-ID: 0'
```

`--once`는 Step 하나만, `--drain`은 현재 due 작업이 없을 때까지 처리합니다. Run 하나에는 Model Step과 Tool Step이 각각 필요합니다. 여러 worker가 Work별 lease로 실행하며, 지속적인 장애 복구에는 일반 worker loop를 사용합니다. `--drain`은 미래 backoff나 아직 만료되지 않은 lease를 기다리지 않습니다.

기존 설치는 [Phase 2b migration 경계](docs/implementation/phase-2b.md#migration과-운영-제약)를 먼저 확인합니다. 실행 중 작업·미확정 효과가 있으면 migration이 거부될 수 있습니다. 기존 데이터베이스에는 자동으로 migration하지 않습니다.

## 검증

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
docker compose config --quiet
```

통합 테스트는 Docker에 **별도 폐기 가능한 PostgreSQL**을 생성합니다. 기존 DB에서 테스트를 실행하거나 Docker 부재 시 SQLite로 대체하지 않습니다. CI도 같은 명령으로 검사합니다.

100건 동시 idempotency 접수, schema·상태 전이, 실제 non-superuser RLS, Project 권한, 별도 API·worker 프로세스, 병렬 worker, SIGKILL 복구, stale commit 거부, 취소/dispatch 경합, provider 결과 재사용, SSE 재접속을 검사합니다. 범위와 제약은 [Phase 2b 구현 현황](docs/implementation/phase-2b.md)에 기록합니다.

## 설계 문서

- [Agent Runtime Platform 엔터프라이즈 설계](docs/architecture/agent-runtime-platform.md)
- [Python-first Runtime Stack 설계](docs/superpowers/specs/2026-09-04-python-runtime-stack-design.md)
- [Phase 0·1 Python 구현 계획](docs/superpowers/plans/2026-09-04-phase-0-1-python-runtime.md)
- [Phase 0·1 구현 현황과 운영 경계](docs/implementation/phase-0-1.md)
- [Phase 2a worker lease·복구 구현](docs/implementation/phase-2a.md)
- [Phase 2a 실행 계획](docs/superpowers/plans/2026-09-13-phase-2a-recovery.md)
- [Phase 2b Tool 효과·취소·조정](docs/implementation/phase-2b.md)
- [Phase 2b 실행 계획](docs/superpowers/plans/2026-09-13-phase-2b-effects-cancellation.md)
- [AI 모델 릴리스 Agent 예제 패키지](examples/ai-model-release/README.md)

## 이후 달성할 운영 목표

- 서버나 워커 장애 후에도 중단된 실행을 안전하게 이어갑니다.
- provider의 idempotency 계약 안에서 도구 중복 효과를 방지하고, 결과가 불명확하면 자동 재시도 대신 조정 절차로 전환합니다.
- 모든 실행을 `run → step → model call → tool call → result` 단위로 추적합니다.
- Tenant와 Project별 권한, 한도, 비용, 데이터 보존 정책을 분리합니다.

## 책임 범위

| 영역 | 책임 |
| --- | --- |
| Control Plane | Tenant·Project·Membership·Connection과 Agent·Tool 불변 버전 |
| Runtime | 상태 전이, 순차 스텝 스케줄링, 중단·재개, 장애 복구 |
| Tool Gateway | 스키마 검증, 인증, 승인, timeout, 감사 로그 |
| Memory | 단기·장기 메모리, 출처, TTL, 사용자·Project·Tenant 격리 |
| Observability | trace 연결, 실패 재현, 민감정보 마스킹 |
| Governance | quota, rate limit, 실행 예산, 무한 루프 방지 |

플랫폼 코어는 특정 업무를 알지 못합니다. 모델 평가·벤치마크·배포 같은 AI 업무는 Agent Definition과 Tool 패키지로 등록하며, 해당 패키지가 없어도 플랫폼은 정상적으로 실행되어야 합니다.

## 구현 스택

- Python 3.13, FastAPI, Uvicorn
- Pydantic v2, SQLAlchemy 2.0 Core, Psycopg 3, Alembic
- PostgreSQL authoritative state와 lease 기반 asyncio workers
- uv, Ruff, Pyright strict
- pytest, pytest-asyncio, Hypothesis, Testcontainers

API와 worker는 같은 Python package를 공유하지만 별도 process로 실행합니다. Domain 계층에서는 FastAPI·Pydantic·SQLAlchemy·Psycopg를 import하지 않습니다.

## 핵심 지표

- 실행 성공률 및 장애 후 복구율
- 중복 도구 실행률
- P95 실행 지연시간
- trace 누락률
- 실행당 토큰·도구 비용

## 업무 백로그

### 1. 실행 코어

- [x] 실행 상태 머신과 전이 규칙 정의
- [x] PostgreSQL 기반 event/run store와 단일 worker 구현
- [x] worker lease, heartbeat, fencing과 mock 작업 회수
- [x] SSE 이벤트 조회·재접속
- [x] cancel/dispatch 직렬화와 결과 확인 기반 취소 상태
- [ ] backpressure 처리

### 2. Durable Execution

- [x] Step checkpoint와 저장된 다음 Work부터 자동 복구
- [x] mock retry, exponential backoff, dead-letter 기록
- [ ] 수동 pause/resume, 권한 기반 DLQ redrive
- [x] Run 접수 idempotency key와 mock 도구 호출 기록
- [x] Tool effect 원장·mock provider idempotency·결과 조정 API
- [ ] 실제 외부 provider의 중복 제거 기간·결과 조회 계약 적용
- [ ] 외부 부작용 실패 시 조회·보상 전략 작성

### 3. Tool Gateway와 보안

- [x] JSON Schema 기반 입력·출력 검증
- [ ] 도구별 권한 scope와 human approval 구현
- [ ] secret 전달 및 외부 네트워크 정책 정의
- [ ] sandbox 실행과 감사 로그 구현

### 4. Memory와 멀티테넌시

- [x] Tenant·Project·Principal·Membership 기반 접수·조회·claim 권한 확인
- [ ] Connection metadata와 secret reference 수명주기 구현
- [ ] 사용자·조직별 메모리 격리
- [ ] 출처, 갱신 시점, TTL, 삭제 정책 구현
- [ ] tenant별 quota, rate limit, 비용 집계

### 5. 추적과 품질 운영

- [ ] OpenTelemetry 기반 run trace 연결
- [ ] prompt/model/tool 버전 기록
- [ ] redaction, sampling, retention 정책 구현
- [ ] 실패 trace를 회귀 평가 데이터셋으로 전환

## 마일스톤

| 단계 | 결과물 |
| --- | --- |
| M1 | 단일 워커에서 실행·도구 호출·상태 저장 |
| M2 | 워커 강제 종료 후 복구 및 중복 실행 방지 |
| M3 | trace viewer, 취소, timeout, DLQ |
| M4 | 멀티테넌시, 비용·quota, 보안 정책 |
| M5 | 실패 재실행과 배포 전 회귀 평가 |

## 최종 운영 단계의 완료 기준 (현재 미충족)

- 워커 강제 종료 시나리오에서 실행이 유실 없이 복구됩니다.
- idempotency를 지원하는 provider에서는 같은 effect가 중복 반영되지 않고, 지원하지 않는 provider의 불명확한 결과는 `OUTCOME_UNKNOWN`으로 격리됩니다.
- 하나의 run을 입력부터 결과까지 trace로 재구성할 수 있습니다.
- Tenant 간 격리와 동일 Tenant 내 Project 권한·비용 분리를 자동 테스트로 증명합니다.
