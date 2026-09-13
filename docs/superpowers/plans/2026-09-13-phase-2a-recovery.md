# Phase 2a: leased execution and mock recovery

승인된 런타임 우선 로드맵의 다음 구현 묶음. Phase 1 baseline: `63a226a`.

## 목표와 경계

PostgreSQL을 권위로 유지하면서 worker crash 후 저장된 Step부터 실행을 재개한다. 물리 Attempt와 논리 Step을 분리하고 만료·교체된 worker의 commit을 차단한다. 실제 provider, 외부 부작용의 exactly-once, 승인·cancel API, 수동 redrive, 운영 배포는 포함하지 않는다. replay는 현재 허용된 deterministic mock에 한정한다.

## 결정

- Work Item에 worker ID, 단조 증가 lease token, DB 시간 기준 expiry, attempt count를 둔다. 재시도마다 같은 Step/Work 아래 새 Attempt를 생성한다.
- claim·heartbeat·완료·실패·재시도는 현재 owner/token/유효 expiry를 검사한다. heartbeat는 만료 lease를 부활시키지 않는다. heartbeat 실패 시 worker는 진행 중 coroutine을 취소하고 DB lease 회수를 기다린다.
- 단일 session advisory lock을 제거한다. 여러 worker가 서로 다른 Work를 claim할 수 있지만 같은 Work의 현재 lease는 하나다. DB fencing은 외부 서비스의 중복 효과 방지가 아니다.
- expired work는 짧은 transaction으로 회수하고 Attempt를 ABANDONED 처리한다. 재시도 가능한 mock만 backoff 후 READY로 되돌리며, attempt 한도 소진 시 FAILED와 DLQ 기록으로 격리한다. 알 수 없는 provider는 OUTCOME_UNKNOWN으로 차단한다.
- 명시적인 RetryableGatewayError만 일반 operation 재시도 대상으로 추가한다. 기존 validation/policy/unknown exception과 operation timeout은 기존 terminal failure 의미를 유지한다. DB commit 실패를 gateway retry로 바꾸지 않는다.
- 성공한 Step의 결과·다음 Work와 checkpoint metadata를 같은 transaction에 저장한다. 재시작 시 이미 성공한 model을 호출하지 않고 준비된 Tool Step에서 계속한다. 수동 pause/resume API나 임의 workflow replay는 아니다.
- 기본 lease 30초, heartbeat 5초, 최대 시도 3회, backoff base 1초(상한 60초). `heartbeat < lease/3`를 검증한다. 설정은 worker별이며 mixed 설정 운영은 지원하지 않는다.

## 작업 순서와 소유권

1. root: 공유 ports·오류 계약, 계획, 통합 fault test와 문서.
2. DB agent: `0002` migration, repository, read-query tables, DB 회귀 테스트. 기존 migration은 수정하지 않는다. 기존 PROCESSING 작업이 있으면 migration을 거부하고 작업 종료·원인 조사를 요구한다.
3. worker agent: poller heartbeat·shutdown, worker CLI·설정, kernel retry 분류와 단위 테스트.
4. root: 통합 검증, 별도 process 강제 종료·복구 테스트, 독립 코드 리뷰, 커밋·push 및 CI 확인.

## 검증 조건

- live lease는 회수되지 않으며 expired heartbeat/complete/fail/retry는 거부한다.
- worker A 만료 → B 재claim → A의 늦은 complete/fail이 B 상태를 바꾸지 못한다.
- 실제 child process를 claim 직후 또는 mock Tool 반환 직후 종료하고 새 worker로 복구한다. 성공한 Model Step은 다시 실행하지 않는다.
- 동시 claim·recovery의 중복 Attempt, checkpoint, terminal event를 방지한다.
- retry backoff와 최대 횟수, DLQ tenant RLS, 기존 65개 회귀를 검사한다.
- `uv run ruff format --check .`, `uv run ruff check .`, `uv run pyright`, `uv run pytest -q`, `uv build`, Compose 구문 검사.

## 적용·되돌리기

로컬·CI disposable PostgreSQL에서만 migration을 검증한다. 기존 사용자 DB에 자동 적용하지 않는다. 앱/worker를 멈춘 뒤 upgrade하는 동시 중지 배포가 필요하다. 이전 worker와 rolling 혼용하지 않는다. downgrade는 새 실행 이력을 지우므로 운영 rollback으로 사용하지 않으며 active leased work가 있으면 거부한다. 원격 변경은 이 저장소 main push만, 클러스터 배포·인증 설정 변경 없음.
