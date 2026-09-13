# Phase 0·1 구현 현황과 경계

작성일: 2026-09-13. 이 문서는 현재 코드의 범위를 설명한다. 장기 설계와 기존 실행 계획의 코드 조각보다 실제 실행 명령·지원 기능에 대해 우선한다.

> 이 문서는 `63a226a`의 Phase 1 기록이다. 이후 추가한 lease·재시도·복구와 현재 migration 경계는 [Phase 2a 구현 현황](phase-2a.md)을 따른다.

## 실행 구조

```text
HTTP POST /v1/runs
  → 인증·Membership·Agent input schema·지원 정책 검증
  → [한 transaction: idempotency + Run + Model Step + Event + Work Item]
  → 202 QUEUED

별도 단일 worker
  → [claim transaction: Work PROCESSING + Step/Run + call intent + Event]
  → transaction 밖에서 mock model 판단
  → [완료 transaction: Model 결과 + Tool Step/Work + Event]
  → 다음 poll에서 Tool Step claim
  → transaction 밖에서 mock Tool 실행·출력 schema 검증
  → [완료 transaction: Tool 결과 + Run COMPLETED + Event + usage]
```

DB commit 뒤에만 접수 성공을 반환한다. 외부 호출 동안 업무 transaction/row lock을 유지하지 않는다. 단일 worker 제한을 위한 별도 session advisory lock connection은 유지한다. SSE는 DB event sequence를 커서로 사용하며, 완료 후 마지막 커서로 재접속하면 종료한다. SSE 재접속은 **실행 재개 기능이 아니다**.

## 구현 범위

| 경계 | 현재 구현 | 아직 없는 기능 |
| --- | --- | --- |
| 계약 | 순수 Python domain, 상태 registry, 불변 Pydantic Agent/Tool 계약, canonical digest | 미래 상태별 scheduler 동작 |
| 접수 | principal에서 Tenant 유도, active Project/Membership 확인, idempotent 원자적 기록 | rate limit, quota, 운영 identity provider |
| 실행 | mock model → 단일 mock Tool, 별도 Step/Work, operation timeout, 오류 격리 | lease, retry, checkpoint, cancel, DLQ, 멀티 worker |
| 저장 | PostgreSQL 17, frozen Alembic DDL, composite FK, 버전 UPDATE/DELETE 차단 | 운영 backup/PITR·retention 절차 |
| 관측 | metadata-only 순서화 Event, 상태 조회, SSE cursor, mock 호출 횟수 usage | OTel export, token·금액 계량, trace viewer |
| 정책 | 미지원 정책·실제 provider를 거부 | 승인, compensation, Run deadline, token·cost budget |

`execution_policy`, `approval_policy`, `compensation_policy`는 미래 계약으로 타입이 존재하지만 Phase 1 접수 시 값이 있으면 거부한다. 현재 제한은 worker 설정의 **operation timeout**과 고정된 두 Step 구조다. Run 전체 deadline이나 queue 대기시간 제한으로 해석하지 않는다. 실제 실행 예제는 `examples/ai-model-release/*.json`; 기존 YAML과 상세 시나리오는 미래 동작 설계다.

## 보안·권한 경계

- API의 local identity는 명시적인 development mode와 토큰을 요구한다. 운영 identity verifier는 아직 없으며, 인증이 구성되지 않으면 실행 API를 허용하지 않는다. HTTPS, OIDC/JWT 검증, secret 관리, 사용자 등록 API는 다음 작업이다.
- DB migration 관리자와 런타임 계정을 분리한다. `agent_app`은 NOLOGIN·NOSUPERUSER·NOBYPASSRLS 역할이다. local installer만 `runtime_local`에 해당 역할을 부여한다.
- RLS는 transaction-local Tenant context로 **Tenant 행 격리**를 수행한다. 같은 Tenant의 Project 접근은 application의 Membership 검사와 composite FK가 담당한다. Project별 RLS라고 주장하지 않는다.
- custom PostgreSQL context는 신뢰된 서버가 설정한다. DB 접속 권한을 최종 사용자에게 주면 임의 context 변경을 막는 보안 경계가 아니다. 런타임 DB 자격 증명은 신뢰된 API·worker만 가진다.
- dispatcher의 SECURITY DEFINER 함수는 fixed search path와 별도 NOLOGIN owner를 사용한다. 실행 payload가 아닌 claim metadata만 반환하며, worker가 같은 transaction 안에서 Tenant context와 상태를 반영한다.
- 조회·이벤트·claim은 active Tenant/Project/Principal/Membership을 확인한다. 권한이 회수된 대기 작업은 외부 호출 없이 실패 처리한다. 이미 claim된 작업의 실행 도중 취소 보장은 아직 없다.
- HTTP 본문은 64KiB 제한, contract는 추가 필드를 거부한다. JSON Schema의 `$ref` 계열은 거부해 외부 schema URL을 따라가지 않는다. 이벤트에는 원문 입력·출력을 넣지 않고, 오류 응답에도 provider 예외 원문을 노출하지 않는다.
- **Run/Step/call의 JSON 입력·결과는 DB에 원문 저장한다. 암호화 artifact store·PII redaction·retention이 없으므로 실제 민감정보를 넣지 않는다.** Event 최소화가 전체 저장 데이터의 익명화를 의미하지 않는다.

## 장애 처리의 한계

operation timeout 또는 Tool exception은 실패 상태를 저장하고 다음 Run을 처리한다. 그러나 worker 프로세스가 `PROCESSING` 저장 뒤 종료되거나 DB가 완료 commit 중 끊어지면 자동 재claim·복구하지 않는다. claim 결과가 불확실한 작업을 수동으로 READY로 되돌리는 절차도 제공하지 않는다. Phase 2에서 lease/fencing과 `OUTCOME_UNKNOWN` 조정을 구현한 뒤 실제 외부 효과를 연결한다.

session advisory lock은 두 번째 worker 시작을 거부하고 매 poll 소유권을 검사한다. 실행 중 lock connection을 잃은 오래된 worker를 fencing하는 완전한 분산 실행 보장은 아니다. 현재 adapter는 deterministic mock이며 외부 변경을 일으키지 않는다.

## 계획에서 조정한 부분

| 기존 계획 | 구현 결정과 이유 |
| --- | --- |
| 예시의 mutable schema metadata로 migration 구성 | migration에 DDL을 고정하고 SQLAlchemy text query를 bound parameter로 실행한다. `tables.py`는 read-query symbol이며 `metadata.create_all`은 지원하지 않는다. |
| Agent/Tool version만으로 논리 정의 표현 | 별도 definition identity와 immutable version을 둔다. principal·actor·event schema_version과 composite Project/Run FK를 보강했다. |
| 단순 worker 반복 | 두 Step을 각각 claim하며 단일 worker advisory lock을 추가했다. retry/lease로 확대하지 않았다. |
| 예제에 미래 execution policy 포함 | 실행하지 못하는 정책은 fail-closed로 거부하고 실행 JSON에서 제외했다. |
| mock 평가 출력이 설계 schema와 다름 | candidate/suite 참조와 점수·decision을 일치시키고 contract test로 고정했다. |
| downgrade를 데이터 보존 rollback으로 취급 | 초기 migration downgrade는 **테이블과 데이터를 제거**한다. disposable DB에서만 round-trip 테스트하며 운영 rollback 수단이 아니다. |

## 검증과 다음 작업

`uv run pytest -q`는 pure-domain/property, contract, 실제 PostgreSQL integration, 별도 HTTP API·worker process 테스트를 수행한다. 주요 회귀는 동시 접수 100건의 단일 Run 보장, 다른 입력의 idempotency 충돌, composite FK, non-superuser RLS/default deny, immutable version, 권한 회수, 실패 이후 계속 처리, SSE sequence/reconnect다. Docker가 필요하며 운영 DB를 사용하지 않는다.

2026-09-13 로컬 검증: 전체 65개 테스트 통과, Ruff lint/format·Pyright strict 통과, wheel/sdist build 성공, Compose 구문·로컬 migration·seed CLI 실행 확인. 독립 리뷰에서 재현한 revoked 작업 이후 drain 조기 종료와 custom 개발 Tenant의 seed/auth 불일치를 수정하고 회귀 검증했다. 이 결과는 mock 최소 실행의 증거이며 운영 배포나 Phase 2 복구의 검증이 아니다.

다음은 Phase 2: lease·heartbeat·fencing → crash/commit ambiguity tests → retry·idempotent Tool effect reconciliation → checkpoint/resume·cancel·DLQ 순서로 진행한다. 외부 LLM/GPU/배포 도구를 먼저 연결해 현재 한계를 숨기지 않는다.

관련 공식 근거: [PostgreSQL RLS](https://www.postgresql.org/docs/17/ddl-rowsecurity.html), [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html), [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/).
