# Phase 2c: 권한 기반 DLQ redrive

## 범위와 불변 조건

- Run별 DLQ metadata 조회와 명시적 redrive API를 추가한다.
- 기존 terminal Run/Step/Attempt/effect 이력은 재개하거나 초기화하지 않는다. 같은 불변 Agent 버전과 원본 입력으로 **새 Run을 처음부터 시작**하며 source DLQ와 연결한다.
- 현재 owner/operator Membership과 활성 Tenant/Project/Principal을 확인한다. 자식 Run은 redrive 요청 Principal의 권한으로 실행한다.
- 허용 대상은 `FAILED` Run/Work + `RETRY_EXHAUSTED` DLQ, cancel epoch 0, 실행 중 Work 없음, 어떤 Tool도 전송한 적 없는 경우다. 원본 mock route와 현재 접수 정책도 재검증한다.
- `OUTCOME_UNKNOWN`, 전송 이력, 취소·성공 실행, 일반 validation 실패, 지원하지 않는 provider는 fail closed. UNKNOWN은 reconcile만 허용한다.
- 원본 DLQ 하나당 자식 하나. 같은 Principal/key/reason 재전송은 같은 자식을 반환하고 다른 조작은 409. 모든 생성·연결·양쪽 감사 event를 하나의 transaction으로 처리한다.
- 요청에는 Idempotency-Key와 제한된 reason code만 허용한다. 입력·Agent 버전·Tenant·Project·결과 증거를 바꿀 수 없다. raw 자유 텍스트 사유를 audit에 보관하지 않는다.

## 구현 순서

1. 계약과 API: `ports.py`, `api/app.py`, contract tests. `GET /v1/runs/{id}/dead-letters` (cursor/limit), `POST /v1/runs/{id}/dead-letters/{item_id}/redrive` (202).
2. Persistence: `repositories.py`, 새 `0004` migration, scoped FK/unique/RLS와 append-only redrive 기록. Work→Run→Step→Effect 잠금 순서를 유지하고 권한/자격 재확인. 실 PostgreSQL에서 경합·rollback·격리·중복 요청 검증.
3. 통합: 실제 API→redrive→worker→완료 테스트, 기존 198개 회귀, migration 호환. 사용자 DB는 변경하지 않는다.
4. 운영 문서: eligibility, UNKNOWN 대응, 재실행 후 관측, migration/rollback과 한계. 독립 리뷰 후 lint/type/build/tests, 커밋·main 푸시·해당 CI 확인.

## 검증

`uv run ruff format --check .`, `uv run ruff check .`, `uv run pyright`, `uv run pytest -q`, `uv build`, `docker compose config --quiet`, `git diff --check`.

승인 기준: 동시 중복 요청에도 자식·redrive 기록 각 하나, 같은 Tenant 다른 Project와 권한 철회 시 거부, 전송/UNKNOWN 재실행 불가, 실패한 transaction에 고아 Run 없음, 원본 상태와 시도 이력 보존, 자식은 새 attempt budget으로 완료.

## 운영 경계

local mock 전용. 전체 Run 재시작이므로 기존 Model 판단을 재사용하는 checkpoint redrive, 실 provider, 자동 일괄 redrive, pause/resume, production RBAC·rate limit은 포함하지 않는다. migration은 additive이며 기존 이력은 수정하지 않는다. downgrade는 redrive 이력을 지우므로 이력이 있으면 거부한다. 기존 0001–0003은 수정하지 않는다.
