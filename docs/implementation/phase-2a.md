# Phase 2a: worker lease와 자동 복구

Phase 1 최소 실행에 **mock 작업의 장애 복구**를 추가한다. 외부 부작용의 exactly-once 보장이나 전체 Phase 2 완료를 의미하지 않는다.

## 무엇이 달라지는가

| 항목 | Phase 1 | Phase 2a |
| --- | --- | --- |
| worker 소유권 | 프로세스 전체 session advisory lock | Work별 owner + lease token + DB expiry |
| 병렬 실행 | worker 하나 | 서로 다른 Work를 여러 worker가 claim |
| worker 종료 | PROCESSING에 남음 | 만료 Attempt 종료 → backoff → 새 Attempt |
| 늦은 결과 저장 | 상태 검사 | 상태·owner·token·expiry 검사로 stale commit 거부 |
| 재시도 | 없음 | 명시적 transient 오류·안전한 mock lease 회수, 유한 횟수 |
| 성공한 Step | 결과와 다음 Work 저장 | 동일 transaction에 checkpoint metadata도 저장 |
| 한도 초과 | 없음 | Run 실패 + dead-letter 기록, 자동 redrive 없음 |

## 실행 흐름과 보장

1. worker가 만료된 lease를 회수한 뒤 due Work를 claim한다. DB 시간으로 lease를 발급하고 새로운 물리 Attempt를 기록한다.
2. gateway 호출과 별도로 heartbeat가 lease를 연장한다. 이미 만료된 lease는 연장하지 않는다. heartbeat 실패·소유권 상실 시 실행 coroutine을 취소한다.
3. 완료·실패·재시도 저장은 현재 lease의 권한을 다시 검사한다. 이전 worker가 늦게 돌아와도 새 worker의 projection을 덮어쓸 수 없다.
4. 성공한 Model Step의 결과·다음 Tool Work·checkpoint를 한 transaction으로 저장한다. 그 다음 프로세스가 종료돼도 Tool Step부터 이어가며 Model Step을 다시 실행하지 않는다.
5. Tool이 반환한 직후, 결과를 DB에 저장하기 전에 죽었다면 결과는 저장되지 않은 것이다. 현재 deterministic mock은 다시 실행해도 외부 효과가 없으므로 새 Attempt로 재실행한다. 실제 provider에는 이 전제를 적용하지 않는다.

checkpoint는 성공한 Step과 고정된 버전·결과 reference의 durable 경계다. 다음 Step은 이미 저장된 Work Item으로 이어진다. 임의 시점의 메모리 snapshot, 수동 pause/resume API, 새 agent 버전으로 원본 Run을 replay하는 기능은 아니다.

## 오류 분류

| 상황 | 동작 |
| --- | --- |
| `RetryableGatewayError` | 안전한 mock만 backoff 후 재시도 |
| worker crash / lease 만료 | 이전 Attempt 종료 후 안전성·시도 한도 검사 |
| 시도 한도 소진 | terminal failure와 `dead_letter_items` 기록 |
| schema 오류·권한 거부·일반 gateway 오류 | 기존과 같이 재시도 없이 실패 |
| operation timeout | `TIMED_OUT`; Run 전체 deadline 구현은 아님 |
| 완료 DB transaction 오류·commit 불확실성 | 성공/실패를 추측하지 않고 오류 전파, DB 상태와 lease 회수로 판단 |
| 알 수 없는 실제 provider 결과 | 자동 replay 금지, `OUTCOME_UNKNOWN` 격리 |

성공한 논리 호출의 `usage_entries`와 물리 실행 횟수인 `run_attempts`는 다르다. 재시도 비용·token 계량은 아직 구현하지 않았다. 모든 외부 provider와 nonnull Agent 실행·승인·보상 정책은 계속 접수 단계에서 거부한다.

## 설정과 실행

`.env.example`에 다음 worker 설정을 추가했다. 모든 worker에 같은 정책을 사용한다.

| 환경 변수 접두사 `AGENT_PLATFORM_` | 기본값 |
| --- | --- |
| `LEASE_SECONDS` | 30초 |
| `HEARTBEAT_INTERVAL_SECONDS` | 5초; 반드시 lease의 1/3 미만 |
| `MAX_ATTEMPTS` | Step당 3회, 최초 시도 포함 |
| `RETRY_BASE_SECONDS` | 1초, 지수 backoff와 jitter, 상한 60초 |

```bash
uv run python -m agent_platform.worker.main
```

여러 터미널에서 worker를 실행할 수 있다. `--once`는 최대 Step 하나, `--drain`은 **현재 due 작업이 없어질 때까지** 처리한다. `--drain`은 아직 만료되지 않은 lease나 미래 backoff의 완료를 기다리지 않는다. 지속적인 복구에는 일반 worker loop를 사용한다.

## Migration 안전

- `0001`을 변경하지 않고 `0002`를 추가한다. API 접수를 중지하고 기존 worker 작업을 마친 뒤 모두 중지하는 방식으로 upgrade한다. 이전 코드와 새 코드를 rolling 혼용하지 않는다.
- 기존 `PROCESSING` Work가 있으면 upgrade를 거부한다. 기존 Phase 1 crash 작업은 원인을 조사하고 상태를 명시적으로 정리할 때까지 migration하지 않는다. migration이 실행 완료를 추측하거나 임의 재전송하지 않는다.
- 새 버전도 `PROCESSING` Work가 있으면 downgrade를 거부한다. downgrade는 Attempt/checkpoint/DLQ 이력을 제거하므로 운영 데이터 보존 rollback 수단이 아니다.
- 로컬 검증은 Testcontainers가 만든 폐기 가능한 DB에서 수행한다. 기존 사용자 DB·운영 클러스터에 자동 migration하지 않는다.

Tenant RLS·Project Membership·불변 버전 경계는 그대로 유지한다. DB 접속 계정은 신뢰된 서버만 사용해야 한다. payload의 암호화·PII redaction·retention은 아직 없으므로 실제 민감정보를 넣지 않는다.

## 검증 범위와 다음 단계

새 테스트는 실제 child process SIGKILL, 저장된 Model Step 재사용, stale complete/fail/retry 거부, heartbeat의 만료 lease 부활 차단, 동시 claim/recovery, retry 한도·DLQ, migration 경계를 확인한다. mock 반환 직후 종료 테스트는 **외부 효과 중복 제거 검증이 아니다**.

2026-09-13 로컬 검증: 전체 130개 테스트 통과, Ruff lint/format·Pyright strict·wheel/sdist build·Compose 구문 검사 통과. 독립 리뷰에서 모델의 도구 인자 schema 오류가 crash retry로 남는 문제를 재현·수정했고, 검증 오류는 terminal failure로, DB commit 불확실성은 lease 복구 대상으로 분리하는 회귀 테스트를 통과했다. 기존 사용자 DB에는 migration하지 않았다.

다음 묶음은 Tool effect ledger·idempotency key·provider 결과 조회와 `OUTCOME_UNKNOWN` 조정, cancel/dispatch 경쟁 제어, 운영자가 권한을 확인하고 수행하는 DLQ redrive다. 실 provider·GPU 연동은 해당 안전 계약을 구현한 뒤 진행한다.

관련 근거: [PostgreSQL row lock](https://www.postgresql.org/docs/17/explicit-locking.html), [Python asyncio task cancellation](https://docs.python.org/3.13/library/asyncio-task.html).
