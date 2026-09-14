# Phase 3a: Durable trace와 평가 후보

Phase 2의 저장 이력을 안전하게 관측하는 첫 단계다. worker 실행 코드를 바꾸지 않고 별도 observation repository와 API를 추가한다. 이것은 **DB 이력의 metadata projection**이며 OpenTelemetry exporter, 모델 성능 측정, 자동 평가 완료를 뜻하지 않는다.

후속 [Phase 3b](phase-3b.md)에서 worker OTel span과 로컬 전송을 별도로 추가했다. 아래는 Phase 3a 완료 시점의 계약·검증이며 DRAFT 후보 경계는 그대로 유지한다.

## Trace 계약

`GET /v1/runs/{run_id}/trace`는 같은 PostgreSQL snapshot에서 다음 관계를 조회한다.

```text
Run (state version, immutable Agent version/digest)
 ├─ Step → Model/Tool call metadata, Tool schema version/digest
 ├─ Work → 물리 Attempt → 시작·종료·duration·정규화된 실패 코드
 ├─ Effect → dispatch Attempt 연결과 확인된 상태
 ├─ Checkpoint → 성공 Step/Attempt 연결
 └─ Event sequence + 저장된 usage
```

조회는 repeatable-read/read-only transaction으로 수행한다. 일반 trace GET은 실행 Work에 row lock을 걸지 않는다. 평가 후보를 만들 때는 source Run 관련 Work→Run→Step→Effect를 잠그고 같은 transaction에서 다시 조회한다.

### Metadata-only 정책

`redaction_policy=metadata-only-v1`은 다음 항목을 응답·후보 snapshot에 복사하지 않는 정책이다.

- Run/Step 입력·출력, model response, Tool 인자·결과, Agent spec 원문
- error message, event payload·actor, worker ID
- provider idempotency key·dispatch token, 입력/결과의 content digest

자유 문자열 error code와 model route 등은 허용 목록으로 정규화한다. 식별자와 불변 Agent/Tool schema fingerprint는 연결 metadata로 허용한다. 이 정책은 임의 식별자에 개인정보를 넣어도 정제해 주는 범용 PII 탐지기가 아니다. 식별자는 비밀정보를 담지 않는 opaque ID여야 한다.

기존 runtime 저장소나 기존 Run/event API의 raw payload 보존 정책을 변경한 것은 아니다. 원문 암호화, opt-in content reference, retention·삭제 정책은 후속 작업이다.

### 수치·완전성의 의미

- Attempt duration은 durable Attempt 시작~종료 간격이다. worker scheduling·실행·결과 저장 시간을 포함하며 provider RTT/TTFT/TPOT와 다르다. 아직 끝나지 않은 Attempt는 확정 duration이 없다.
- usage는 현재 commit된 논리 호출 기록이다. 재시도한 모든 물리 호출의 청구량이나 실제 token/cost가 아니다. 수집하지 않은 측정값은 0으로 꾸미지 않고 null로 둔다.
- Agent version/digest, Tool schema version/digest, 저장된 mock model route를 확인할 수 있다. 독립 prompt/runtime build version과 실 provider model revision은 아직 수집하지 않는다.
- collection별 최대 1000개까지만 반환한다. 초과 시 `integrity.truncated=true`, `complete=false`다. event sequence gap, Work의 attempt count와 실제 이력 불일치, 연결 누락도 integrity issues로 표시하며 불완전한 snapshot은 후보로 저장하지 않는다.
- `integrity.complete=true`는 현재 검사하는 durable row 관계와 event sequence가 일치한다는 뜻이다. 외부 provider에 실제로 어떤 일이 일어났는지 증명하지 않는다. `OUTCOME_UNKNOWN`은 그대로 불확실한 상태다.

## 평가 후보: 승인되지 않은 불변 DRAFT

terminal 또는 `OUTCOME_UNKNOWN` Run의 metadata snapshot을 검토용 후보로 저장한다. 아직 실행 중인 Run이나 불완전한 trace는 거부한다.

```bash
curl -sS 'http://127.0.0.1:8000/v1/runs/<run_id>/trace' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy'
curl -sS -X POST 'http://127.0.0.1:8000/v1/runs/<run_id>/evaluation-candidates' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy' \
  -H 'Idempotency-Key: candidate-review-001' \
  -H 'Content-Type: application/json' \
  -d '{"source_state_version":7,"expected_state":"COMPLETED"}'
curl -sS 'http://127.0.0.1:8000/v1/evaluation-candidates/<candidate_id>' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy'
```

예제 version `7`은 실제 trace 응답의 `run.state_version`으로 바꾼다. 기대 상태는 `COMPLETED`, `FAILED`, `CANCELLED`, `TIMED_OUT`, `REJECTED` 중 하나다. 기대 상태는 운영자가 검토하려는 값이며 실제 결과나 모델 품질 판정으로 취급하지 않는다.

서버가 현재 권한·source version·완전성을 확인하고 metadata snapshot, digest, redaction policy와 출처 version을 고정한다. 클라이언트가 입력·snapshot·Tenant·Project·APPROVED 상태·성공 증거를 제출할 수 없다. 원본 Run/event는 변경하지 않고 provider/model 호출도 발생하지 않는다.

첫 생성은 `201`, 같은 scoped key·Run·version·기대 상태의 재요청은 `200`과 `duplicate=true`다. 같은 key의 다른 요청은 `409`. source version이 바뀌었으면 다시 trace를 검토해야 한다. 이미 접수된 요청을 같은 key로 재전송하면 source가 나중에 reconcile되더라도 기존 snapshot을 돌려준다. 이때도 현재 권한을 다시 확인한다.

후보 GET/생성/trace GET 모두 활성 owner/operator Membership과 source Project 접근을 확인한다. 다른 Tenant·권한 없는 Project·권한 철회는 `404`로 처리한다. DB에도 Tenant RLS와 Project 소유권 composite FK를 둔다.

후보는 항상 `DRAFT`이며 수정·삭제 API와 승인 API가 없다. **학습 데이터, golden set, 재생 가능한 evaluation case, release gate가 아니다.** 실제 평가에 필요한 정제된 입력, expected result/trajectory, scorer/rubric, 개인정보·품질 검토와 버전별 회귀 runner는 다음 단계에서 명시적으로 연결해야 한다.

## Migration과 운영 경계

새 `0005` migration은 evaluation candidate 저장소만 추가한다. 기존 migration과 사용자 DB를 자동 수정하지 않는다. disposable PostgreSQL에서 테스트하며 실제 설치의 upgrade는 이전 [migration 경계](phase-2b.md#migration과-운영-제약)를 따른다. 후보가 있으면 downgrade가 이력 손실을 막기 위해 거부된다.

OTel context/exporter/drop metric, provider latency/token/cost 계측, sampling/retention, trace UI, 후보 검토·dataset 승격·offline replay·회귀 gate는 아직 미구현이다. 관측 조회는 실행의 권위 상태를 대체하지 않는다.

## 검증 결과

2026-09-14 로컬 전체 **272 tests passed**(기존 229개 + 신규 43개). Ruff format/lint, Pyright, frozen dependency sync, source/wheel build, Compose config와 diff 검사 통과.

실제 disposable PostgreSQL에서 snapshot 일관성·원문 canary 배제·상태 정규화·1000건 제한·event/attempt 누락·권한 철회·동시 생성·reconcile 경합·snapshot 불변성·RLS/FK·migration을 검사했다. 기본 API composition으로 Run 실행→trace→후보 생성/조회 E2E도 통과했다.

독립 리뷰에서 자유 상태 문자열 노출과 삭제된 Attempt를 완전한 이력으로 오인하는 문제를 발견해 수정하고 회귀 테스트를 추가했다. 최종 집중 42개 테스트와 재리뷰에서 미해결 blocker를 발견하지 못했다. 운영 배포나 실제 provider 관측을 검증한 결과는 아니다.
