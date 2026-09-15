# Phase 3c: 정제 case와 모델 판단 회귀 평가

검토자가 명시적으로 제출한 입력·기대 도구 호출을 불변 case로 저장하고, 고정된 case 목록으로 **mock 모델의 도구 선택·인자**를 비교한다. 기존 DRAFT를 자동 승인하거나 원본 Run을 재실행하지 않는다. 현재 실제 LLM 성능 평가·전체 에이전트 trajectory 평가·배포 gate는 아니다.

## 책임과 버전 경계

```text
metadata-only DRAFT candidate (변경 없음)
  → 검토자가 정제 입력 + 기대 ToolInvocation + 출처 digest 제출
  → 별도 immutable EvaluationCase (explicit-curation-v1)
  → 선택한 case ID/content digest의 suite manifest
  → mock ModelGateway.decide만 호출
  → tool-invocation-exact-v1 비교 → 원문 없는 JSON report
```

- candidate의 `expected_state`는 Run 상태에 대한 검토 메모다. 모델 판단의 정답으로 자동 변환하지 않는다. case의 `expected_decision`은 검토자가 별도로 작성한다.
- `review_confirmed=true`는 인증된 제출자의 정제 확인이다. 독립 검토자의 승인·PII 검사·golden dataset 승격을 의미하지 않는다. 제출자 principal과 생성 시각은 case 저장소에 기록한다.
- 원본 snapshot digest, Agent version/digest, Project, 원본 Run 연결을 확인하고 현재 허용된 mock route의 Agent 입력 schema로 정제 입력을 검증한다. 기대값은 `ToolInvocation` 형태와 source Agent의 tool allowlist를 검증한다. 도구 인자별 실행 schema 검증이나 도구 실행은 scorer의 책임이 아니다.
- case는 수정·삭제 API가 없고 DB trigger도 UPDATE/DELETE를 거부한다. 고칠 때는 새 idempotency key로 새 case를 만든다. 원래 case가 자동으로 폐기·대체되지는 않으므로 평가 목록에서 명시적으로 선택한다.
- `OUTCOME_UNKNOWN` 출처도 모델 판단 검토용 case로 만들 수 있다. 이는 원래 Tool 효과가 안전하거나 실패했다고 확정하는 것이 아니다. redrive·reconcile·취소 상태는 바꾸지 않는다.

## API

| 경로 | 계약 |
| --- | --- |
| `POST /v1/evaluation-candidates/{id}/cases` | source digest·정제 입력·기대 호출·검토 확인을 받아 case 생성. Idempotency-Key 필수. 최초 201, 동일 요청 200, 같은 scoped key의 다른 내용 409. |
| `GET /v1/evaluation-cases/{id}` | 현재 Tenant/Project 권한으로 case 조회. 정제 입력·기대값을 포함하므로 report와 공개 범위가 다르다. |
| `POST /v1/evaluations/offline` | 같은 Project의 서로 다른 case 1~50개를 평가. 현재 유일한 model revision은 `mock/release-planner-v1`. DB report/job을 만들지 않고 200 JSON 응답을 반환한다. |

없는 항목·다른 Tenant·Project 미권한·철회된 Membership은 404다. 명시적 검토 누락·잘못된 JSON/범위·중복 case ID·지원하지 않는 model revision은 422다. 유효한 source digest 형식이지만 실제 후보와 다르면 409다.

case 생성·조회는 현재 활성 owner/operator와 Tenant/Project/Principal 상태를 확인한다. DB는 tenant RLS, scoped FK, SELECT/INSERT 권한을 사용한다. Project 접근은 애플리케이션에서 확인하며 DB 접속 계정 자체는 신뢰된 서비스 경계다.

suite는 단일 repeatable-read/read-only snapshot에서 case와 권한을 읽는다. 모델 await 동안 DB transaction을 잡지 않고, 보고서를 반환하기 전에 새 snapshot으로 권한과 선택된 case digest를 다시 확인한다. 권한 검사와 철회를 직렬화하는 lock은 추가하지 않았다. 마지막 검사 이후의 철회까지 원자적으로 차단하는 계약은 아니다.

## 비교 규칙과 보고서

`tool-invocation-exact-v1`은 tool_version과 arguments의 JSON 값을 비교한다. object key 순서는 무시하고 배열 순서는 유지한다. JSON 숫자는 `1`, `1.0`을 같은 값으로 보며 `-0.0`은 `0`으로 정규화한다. bool `true`와 숫자 `1`, 문자열 `"1"`은 서로 다르다.

Python JSON→PostgreSQL JSONB 저장 과정에서 `1e20` 같은 지수형 숫자가 정수 표현으로 바뀔 수 있으므로 case hash와 scorer에 동일한 정규화를 적용한다. 정수값 float는 `Decimal(str(value))`를 통해 변환해 이진 부동소수점 오차를 정수로 확장하지 않는다. 이는 이 프로젝트의 지원 JSON 규칙이며 범용 RFC 8785 정규화 구현을 뜻하지 않는다.

| outcome | 의미 |
| --- | --- |
| `PASS` | 모델이 허용된 도구명과 기대 JSON 인자를 반환함 |
| `MISMATCH` | 허용된 호출 형식이지만 기대값과 다름 |
| `INVALID_OUTPUT` | 잘못된 ToolInvocation, allowlist 밖 도구, JSON/크기 제한 위반 |
| `PROVIDER_ERROR` | 모델 호출 중 오류. 원문 메시지는 반환하지 않음 |
| `TIMED_OUT` | 모델 호출이 timeout 경로로 종료됨 |

`total`, `passed`, `failed`는 case 수이며 품질 백분율·안전성 점수·통계적 유의성을 뜻하지 않는다. `failed`에는 MISMATCH뿐 아니라 invalid/error/timeout도 포함된다. task 취소는 실패 case로 삼키지 않고 취소를 전파한다.

보고서에는 case별 content/source digest, source Agent ID, 고정 model/scorer revision, 정렬된 suite manifest와 digest가 들어간다. input/output·실제 응답·예외 메시지·stack은 포함하지 않는다. digest는 서명이나 익명화가 아니며 연결 가능한 민감 metadata로 취급한다. `suite_digest`는 case 집합을 식별하고 model/scorer revision은 보고서의 별도 필드다.

현재 mock의 정상 결과는 같은 case 집합에서 순서에 관계없이 재현된다. source Agent version은 출처이지 이번에 실행한 모델 revision 자체가 아니다. 실제 모델의 weights/provider revision, prompt/runtime build pinning과 두 실 모델 버전 비교는 아직 구현하지 않았다. 모델 구현·비교 의미를 바꿀 때는 해당 revision도 변경해야 한다.

## 입력·실행 제한

- 요청 본문은 기존 API의 64 KiB 제한을 따른다. 각 case의 정제 input+expected_decision은 숫자를 정규화한 UTF-8 JSON 기준 16 KiB, 깊이 12, 방문 node 2048을 넘을 수 없다. NaN/Infinity, 비 JSON 값, PostgreSQL이 지원하지 않는 NUL은 거부한다.
- 입력과 기대값을 첫 await 전에 복사하고 case digest를 검사한다. 모델에는 별도 입력 복사본만 넘겨 모델 측 변형이 정답·저장 case를 바꾸지 못하게 한다.
- API는 in-process mock만 선택한다. 임의 provider URL, Python 코드, ToolGateway, RuntimeKernel, worker queue는 호출하지 않는다. offline은 DB까지 없는 로컬 파일 작업이라는 뜻이 아니라 외부 모델·도구 효과가 없는 평가라는 뜻이다.
- 현재 모델별 timeout은 1초이며 최대 50개를 순차 처리한다. asyncio timeout은 협력적 취소이므로 event loop를 막거나 취소를 무시하는 외부 코드를 강제 종료하는 sandbox가 아니다. 그런 코드를 등록하는 API는 제공하지 않는다.
- 보고서는 응답으로만 존재한다. 연결 종료·프로세스 장애 시 report는 유실될 수 있고, 평가 job 상태·작업 lease·보고서 저장·재조회 API는 없다. 재요청은 mock 비교를 다시 수행하지만 runtime 이력이나 Tool 효과를 생성하지 않는다.

## 사용 예

새 설치는 README의 로컬 설정을 따른다. 기존 설치는 migration 경계를 먼저 확인한다. 아래 `<candidate_id>`와 `<snapshot_digest>`는 기존 candidate GET 응답에서 가져오며, 원본 입력을 복사하지 말고 검토한 합성/정제 값을 직접 작성한다.

```bash
curl -sS -X POST 'http://127.0.0.1:8000/v1/evaluation-candidates/<candidate_id>/cases' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy' \
  -H 'Idempotency-Key: curated-case-001' \
  -H 'Content-Type: application/json' \
  -d '{"source_snapshot_digest":"<snapshot_digest>","review_confirmed":true,"input":{"candidate_model_ref":"mock://curated-v1","evaluation_suite_ref":"mock://suite-v1"},"expected_decision":{"tool_version":"evaluation.run_suite:v1","arguments":{"candidate_model_ref":"mock://curated-v1","evaluation_suite_ref":"mock://suite-v1"}}}'

curl -sS -X POST http://127.0.0.1:8000/v1/evaluations/offline \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy' \
  -H 'Content-Type: application/json' \
  -d '{"case_ids":["<case 응답의 id>"],"model_revision":"mock/release-planner-v1"}'
```

기대값의 `candidate_model_ref`만 다르게 쓴 두 번째 case를 새 키로 생성해 함께 평가하면 PASS와 MISMATCH를 구분하는 예제를 만들 수 있다. 이는 모델 품질이 좋아졌거나 나빠졌다는 증명이 아니라 비교 경로의 동작 확인이다.

## Migration·보안·후속 범위

`0006`은 evaluation_cases와 source 연결 제약을 추가한다. 기존 Run/candidate 데이터는 변경하지 않는다. schema 변경은 기존 테이블의 DDL lock을 필요로 하므로 실제 설치에서는 별도 migration 계획이 필요하다. 테스트는 폐기 가능한 PostgreSQL에만 적용했고 사용자 DB를 업그레이드하지 않았다. case가 있으면 downgrade는 보존을 위해 거부한다.

정제 내용은 현재 DB에 평문 JSON으로 저장된다. 자동 PII 탐지·암호화 키 수명주기·보존/삭제·독립 승인 흐름을 제공하지 않으므로 비밀정보·개인정보를 제출하지 않는다. 이 단계의 curated case를 운영용 승인 데이터셋으로 취급하지 않는다.

다음 범위는 case 검토·비활성화/승격 정책, 영속 dataset/report 버전, 회귀 baseline 비교·판정 계약이다. 이후 실제 Model Gateway revision과 연결한다. 현재 결과만으로 자동 배포·rollback을 결정하지 않는다.

## 검증

실제 PostgreSQL에서 concurrent idempotency, scoped FK/RLS, 권한 철회, source digest 변조, 요청 중첩 객체 복사, immutable trigger와 protected downgrade를 검증한다. JSONB 숫자 표현 변경도 생성·재조회·재요청·평가까지 검사한다. E2E는 Run→DRAFT→두 case→PASS/MISMATCH 보고서 흐름에서 Run/event/Attempt/Tool 효과가 추가되거나 변경되지 않는지 확인한다.

2026-09-15 로컬 전체 **426 tests passed**(기존 353개 + 신규 73개). Ruff format/lint, Pyright, frozen dependency sync, source/wheel build, Compose config, diff 검사를 통과했다. 독립 리뷰에서 JSONB 숫자 표현으로 인한 digest 불일치를 재현해 수정했고, 재리뷰의 집중 72개 테스트에서도 추가 차단 문제를 발견하지 못했다. 기존 migration 보존 테스트 두 곳은 head 버전을 `0006`으로 갱신했다. 사용자 DB migration·운영 배포·실제 LLM 평가 결과를 의미하지 않는다.
