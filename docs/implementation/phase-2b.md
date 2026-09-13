# Phase 2b: Tool effect, cancel, reconcile

Phase 2a의 worker lease에 Tool 효과 기록·provider 중복 제거·취소 경합 제어를 추가한다. 실제 provider와 운영 클러스터는 연결하지 않는다.

## 핵심 경계

```text
Model 결과 저장
  └─ 같은 transaction: 다음 Tool Work + effect PREPARED
Tool claim: 실행 소유권만 획득
  └─ begin_tool_dispatch: live lease + cancel epoch + effect 확인
       └─ effect DISPATCHED commit (취소와의 순서를 결정)
            └─ provider 호출: 별도 transaction, 같은 idempotency key
                 └─ runtime 결과·effect SUCCEEDED·checkpoint commit
```

Tool Step의 논리 effect와 물리 Attempt는 다르다. 재시도 시 Attempt/lease가 바뀌어도 effect ID와 provider key는 유지한다. key에 연결된 Tool 버전·인자의 digest가 다르면 거부한다.

각 전송 허가는 `TOOL_DISPATCHED` event에 effect ID·Attempt ID·dispatch token만 기록한다. Run 상태를 가짜로 전이하지 않고 event sequence를 증가시켜 재전송 이력을 보존한다. effect의 Attempt 참조는 Tenant·Project·Run·Step까지 포함한 composite FK로 다른 실행의 Attempt 연결을 거부한다.

기본 worker는 `PersistentMockEvaluationTool`을 사용한다. `mock_provider_results`는 runtime 결과 저장과 별도 transaction이고 runtime row의 FK에 종속되지 않는다. provider 성공 뒤 worker를 강제 종료해도 기록이 남아 재전송·조회에 재사용된다. 여러 동시 요청은 하나의 저장 결과로 합쳐진다.

**이 provider는 동일 PostgreSQL을 이용한 simulator다. 독립 네트워크·독립 저장소 장애, 실제 결제·배포 효과, 실 provider key 보존기간을 검증한 것이 아니다.** 테스트가 보장하는 것은 simulator 결과가 한 번 기록되고 runtime이 같은 논리 key를 사용한다는 것이다. 재시도 중 순수 mock 계산은 반복될 수 있다.

## 취소와 전송의 순서

| 먼저 확정된 상태 | 취소 결과 | 뒤늦은 worker |
| --- | --- | --- |
| 아직 dispatch 전 | `CANCELLED`, `NO_EFFECT` | lease fencing과 dispatch 검사로 전송 거부 |
| `DISPATCHED` 후 | `OUTCOME_UNKNOWN` | 새 dispatch는 금지; 이미 허가된 in-flight 호출은 성공할 수 있음 |
| 이미 terminal Run | 기존 상태 그대로 | 취소가 성공한 실행을 소급해 되돌리지 않음 |

취소는 `cancel_epoch`를 기록하고 현재 lease를 무효화한다. 취소·dispatch의 공유 guard를 잠금으로 직렬화한다. 반복 취소는 새 epoch/event를 만들지 않는다. 도구 전송에 앞서 claim했다는 사실만으로 외부 실행을 허용하지 않는다.

취소 뒤에 network send가 보이더라도 취소 전에 DISPATCHED commit을 완료한 호출이면 기존 in-flight 작업이다. 보장하는 것은 **취소 확정 뒤 새 dispatch 권한을 부여하지 않는 것**이지 이미 실행 중인 외부 효과를 물리적으로 취소하는 것이 아니다.

## 결과 불확실성과 조정

Tool dispatch 뒤 timeout·provider 예외·출력 schema 오류가 발생하면 `OUTCOME_UNKNOWN`에 보관한다. 실패했다고 추측해 새로운 key로 호출하지 않는다. Model의 validation 오류·timeout과 Tool dispatch 전 validation 오류는 기존 terminal failure 규칙을 유지한다.

`POST /v1/runs/{run_id}/reconcile`은 다음 순서로 처리한다.

1. 현재 Principal의 Tenant·Project 접근을 확인하고 해당 Run의 pending effect를 읽는다.
2. DB 잠금 없이 provider의 저장 결과를 조회한다. 클라이언트가 보낸 성공 결과는 받지 않는다.
3. 저장된 결과가 있으면 effect snapshot·현재 상태·권한·schema를 다시 확인하고 원자적으로 반영한다.
4. 취소 요청이 없으면 `COMPLETED`; 취소 요청이 있으면 `CANCELLED`와 `EFFECT_SUCCEEDED`를 기록한다. 이미 발생한 효과를 취소·보상했다고 표시하지 않는다.
5. 결과가 없으면 UNKNOWN을 유지한다. 조회 시점에 없다는 사실은 이전 in-flight 요청이 나중에 성공하지 않는다는 증거가 아니다.

재조정은 멱등적이다. 완료된 조정을 반복해도 event·usage·checkpoint를 중복 생성하지 않는다. provider 조회 장애는 안전한 `503 PROVIDER_UNAVAILABLE` 응답으로 반환하고 원문 provider 예외를 노출하지 않는다.

## API 사용

기존 로컬 실행 방법은 [README](../../README.md)를 따른다. 두 API는 body를 생략하거나 `{}`만 전달한다. Tenant, Project, provider 결과, 성공 여부를 body로 지정할 수 없다.

```bash
curl -sS -X POST 'http://127.0.0.1:8000/v1/runs/<run_id>/cancel' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy'
curl -sS -X POST 'http://127.0.0.1:8000/v1/runs/<run_id>/reconcile' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy'
```

취소 응답의 HTTP `202`는 즉시 효과가 사라졌다는 뜻이 아니다. `state`, `cancel_epoch`, `cancellation_outcome`을 확인한다. 기존 GET Run에서도 같은 필드를 반환한다. 접근 권한 없는 Run의 취소·조정은 `404`로 처리한다.

## Migration과 운영 제약

- 기존 migration을 고치지 않고 `0003`을 추가한다. API/worker를 중지하고 active Work와 미확정 효과를 확인한 뒤 upgrade한다. 구·신 worker를 rolling 혼용하지 않는다.
- migration guard가 활성 작업·미확정 상태를 거부하면 SQL로 READY/성공을 강제 변경하지 않는다. 원인과 결과를 확인하는 별도 운영 판단이 필요하다.
- downgrade는 새 effect/provider 이력을 지울 수 있어 운영 데이터 보존 rollback이 아니다. 미확정 효과가 있으면 거부한다.
- 이 작업에서는 disposable PostgreSQL에서만 migration을 시험하며 기존 사용자 DB에는 적용하지 않는다.
- mock provider는 보존기간 만료 없이 key를 유지한다. 실제 provider 연결 전 dedupe TTL, authoritative lookup, 결과 증거·compensation 정책을 별도로 정해야 한다.
- 기존 raw payload 저장 제한은 그대로다. 운영 identity, 암호화·redaction·retention, tenant budget, sandbox, 실제 provider는 아직 구현하지 않았다.

## 검증 범위와 다음 작업

계약·DB·프로세스 테스트로 cancel-before-dispatch, dispatch-before-cancel, 동시 경합, stale worker 차단, provider 성공 직후 SIGKILL과 같은 key 복구, key/인자 충돌, 결과 없는 조정 유지, 타 Tenant 접근 거부를 확인한다. 전체 Phase 2의 운영 SLA나 실 provider 장애 복구 완료로 확대하지 않는다.

2026-09-13 로컬 검증: 전체 **198 tests passed**, Ruff format/lint, Pyright, frozen dependency sync, wheel/source build, Compose config, diff 검사 통과. PostgreSQL 17 disposable DB에서 migration guard·기존 Tool backfill·downgrade도 시험했다. 별도 에이전트의 독립 리뷰 및 effect/schema 19개 재검증에서 확정적 결함을 발견하지 못했다. 이는 실제 provider 또는 운영 배포 검증이 아니다.

다음은 권한 기반 DLQ redrive와 수동 운영 절차, 이후 trace/평가 데이터 연결이다. 실제 Tool provider는 idempotency/lookup 계약과 권한·secret/egress 경계를 구현한 뒤 연결한다.

관련 근거: [PostgreSQL 행 잠금](https://www.postgresql.org/docs/17/explicit-locking.html), [Python task 취소](https://docs.python.org/3.13/library/asyncio-task.html).
