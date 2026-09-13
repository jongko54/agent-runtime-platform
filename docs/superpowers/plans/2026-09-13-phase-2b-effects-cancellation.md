# Phase 2b: effect ledger, cancellation and reconciliation

Baseline `619e44e`, 기존 에이전트 분업·구현·push 작업의 다음 묶음.

## 목표와 비목표

Tool의 논리 effect ID·인자 digest·dispatch 기록을 추가하고 cancel과 dispatch를 DB에서 직렬화한다. 성공 후 runtime commit 전에 죽어도 동일 key의 mock provider 결과를 재사용한다. 실제 provider, 분산 exactly-once, compensation, 운영 인증, DLQ redrive는 범위 밖이다.

## 계약

- `tool_effects`는 Tool Step당 하나. stable effect ID, request hash, PREPARED/DISPATCHED/SUCCEEDED/OUTCOME_UNKNOWN/CANCELLED, dispatch token을 둔다. Model 완료·Tool Work 생성과 PREPARED 생성은 같은 transaction이다.
- claim은 실행 소유권만 부여한다. gateway 호출 직전 `begin_tool_dispatch(work)`가 현재 lease와 cancel epoch를 검사하고 DISPATCHED를 commit한다. DB transaction과 provider 호출은 별개다.
- `EffectDispatch(id,tenant_id,project_id,idempotency_key,dispatch_token,tool_version,arguments)`를 gateway에 전달한다. 동일 key·다른 인자는 거부한다.
- default worker는 `PersistentMockEvaluationTool(engine)`을 사용한다. provider 결과는 별도 `mock_provider_results` 테이블·독립 transaction으로 저장한다. 같은 DB를 사용하는 **프로토콜 시험용 simulator**이며 실제 네트워크 장애·독립 provider 운영 검증이 아니다. 순수 계산용 MockEvaluationTool은 기존 contract 시험용으로 유지한다.
- Tool dispatch 후 예외/응답 schema 불일치는 성공·실패를 추측하지 않고 OUTCOME_UNKNOWN에 보관한다. 명시적 reconcile은 provider의 저장된 결과만 사용하며 caller가 성공 증거를 제출할 수 없다. 조회 결과가 없으면 UNKNOWN을 유지한다.
- lease 만료 시 PREPARED는 재시도 가능, DISPATCHED는 지원된 idempotent mock만 같은 effect key로 재전송할 수 있다. 미지원 provider는 blind retry하지 않는다.
- `cancel_run(principal,run_id)`는 권한 확인 뒤 cancel_epoch를 증가시키고 lease를 fencing한다. dispatch 이전이면 CANCELLED/NO_EFFECT, 이미 DISPATCHED이면 OUTCOME_UNKNOWN이며 후속 lookup이 필요하다. terminal Run이나 이미 요청한 cancel은 반복해도 상태를 추가 변경하지 않는다.
- 성공 증거를 reconcile하면 일반 UNKNOWN은 COMPLETED, cancel이 있었으면 CANCELLED/EFFECT_SUCCEEDED로 기록한다. 취소가 이미 일어난 효과를 되돌렸다고 표현하지 않는다.
- lock 순서는 기존 Work→Run→Step→Effect와 충돌하지 않아야 한다. cancel의 Work 집합 변경 race도 처리한다. 긴 외부 I/O 동안 lock 없음.

## 분업

1. root: ports, API cancel/reconcile 경계, provider lookup 조합, 프로세스 crash·cancel 경합 E2E, 문서와 최종 검증.
2. persistence agent: `0003`, effect/cancel/reconcile repository, 기존 claim/lease recovery 통합, DB 회귀. `0001`·`0002` 변경 금지.
3. gateway agent: persistent mock provider adapter, kernel dispatch/unknown 처리, worker 연결, 단위·provider contract 시험.
4. 독립 review 이후 전체 테스트·lint·typecheck·build·push·CI 확인.

## 검증

- cancel 먼저 → begin dispatch 거부, provider effect 0건.
- dispatch 먼저 → cancel은 UNKNOWN, 확인된 provider 결과로만 CANCELLED/EFFECT_SUCCEEDED.
- provider 성공 후 SIGKILL → 새 worker에서 같은 key 사용, provider 저장 효과 1건.
- 동일 key 다른 인자 거부; 여러 worker 동시 execute 결과 1건.
- stale lease·다른 Tenant/Project의 cancel/reconcile 거부; 원문 예외 노출 없음.
- 결과 없는 lookup은 UNKNOWN 유지, 재조정 반복은 중복 event·usage·checkpoint 생성 없음.
- migration은 active work/미확정 effect가 있으면 fail closed. 사용자 DB 자동 migration·운영 배포 없음.

명령: `uv run pytest -q`, `uv run ruff format --check .`, `uv run ruff check .`, `uv run pyright`, `uv build`, `docker compose config --quiet`.
