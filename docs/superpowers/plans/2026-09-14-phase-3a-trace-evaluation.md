# Phase 3a: Durable trace와 검토 전 평가 후보

## 범위

기존 Phase 3 설계 중 raw content 기본 비수집, durable 식별자 연결, 실패 분류, trace-to-evaluation 출처 보존을 먼저 구현한다. 별도 PostgreSQL 관측 repository를 추가하여 worker와 실행 코어를 바꾸지 않는다.

- GET Run trace: Run/Step/물리 Attempt/Model·Tool metadata/Effect/Checkpoint/Event/usage를 동일 DB snapshot으로 조회한다.
- trace는 DB 이력의 projection이다. 실제 OTel span이나 provider 호출 latency가 아니다. Attempt duration은 lease attempt의 시작~종료 시간이며 provider RTT와 구별한다. token/cost/prompt/runtime version 등 미수집 필드는 null과 명시적 한계로 표시한다.
- payload·error message·event payload/actor·worker ID·provider key/token·content digest를 복사하지 않는다. error code/model route 등 자유 문자열도 allowlist로 정규화한다. immutable Agent/Tool 식별자·schema digest를 연결한다.
- 조회마다 owner/operator와 Tenant/Project 범위를 확인한다. 각 collection은 최대 1000개, 초과 여부와 event sequence 누락 등 integrity를 명시한다. 불완전한 trace는 평가 후보로 저장하지 않는다.
- POST 평가 후보: source_state_version + expected_state + Idempotency-Key만 받는다. 현재 권한, source version, terminal 또는 OUTCOME_UNKNOWN 상태 확인 후 서버에서 metadata snapshot을 생성한다. 클라이언트 원문/판정 증거를 받지 않는다.
- 후보는 DRAFT immutable record이며 executable evaluation case나 승인된 dataset이 아니다. 입력·기대 결과 내용과 trajectory/rubric은 검토·승격 단계가 필요하다. 자동 replay/model/provider 호출 없음.
- snapshot과 digest·redaction policy·출처 version을 고정하고, 같은 scoped key와 같은 요청은 같은 후보를 반환한다. 다른 요청은409. 후보 저장은 runtime event나 상태를 변경하지 않는다.

## 구현 순서/소유권

1. 공통 계약 `application/observability.py`와 API wiring/contract tests: root.
2. `adapters/postgres/trace.py` metadata projection + read-only snapshot + scope/integrity + tests/integration/test_trace.py: trace agent.
3. `adapters/postgres/observations.py`, migration0005, tests/integration/test_evaluation_candidates.py: persistence agent. trace helper를 사용한다.
4. root HTTP→실제 DB→후보 조회 E2E, 운영문서/README, 독립 read-only 리뷰.
5. lint/type/fulltests/build/Compose, commit/main push/해당 CI 검증.

## 검증과 운영 경계

실제 PostgreSQL에서 정상/복구/UNKNOWN trace linkage, payload secret canary 부재, event gap/truncation, scope/권한 철회, 일관된 조회, 동일키 동시요청1개, stale version409, snapshot불변성, RLS/FK/append-only/migration을 검사한다. 기존229tests 회귀 유지.

`uv run ruff format --check .`, `uv run ruff check .`, `uv run pyright`, `uv run pytest -q`, `uv build`, `docker compose config --quiet`, `git diff --check`.

새0005만 추가한다. 이전 migration/사용자 DB/운영 클러스터 변경 없음. 후보가 있으면 downgrade 거부. OTel/exporter/sampling/retention과 실provider metric, 개인정보 정제 승인·dataset/replay/gate는 후속 단계다.
