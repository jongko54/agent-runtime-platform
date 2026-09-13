# Phase 2c: 권한 기반 DLQ redrive

DLQ는 재시도 한도를 소진했거나 결과를 확정하지 못한 Work의 격리 기록이다. 목록에 있다는 사실만으로 안전한 재실행 대상이 되지는 않는다.

## 재실행 의미

이번 redrive는 **새 Run을 처음부터 시작**한다. 원본 Run의 terminal 상태, Step, Attempt, checkpoint, effect key를 초기화하지 않는다. 원본의 불변 Agent 버전과 입력을 그대로 사용하며, 자식은 요청한 운영자 Principal로 실행한다. 성공했던 Model Step도 다시 실행할 수 있으므로 checkpoint에서 이어가는 기능과 다르다.

```text
원본 Run FAILED + RETRY_EXHAUSTED DLQ
  → 현재 권한·원본 상태·도구 전송 이력 확인
  → 한 transaction: 새 Run/Work + redrive 연결 + 양쪽 감사 event
  → 새 worker attempt budget으로 Model → Tool 실행
원본 Run은 FAILED 유지, 새 Run의 결과는 별도로 관측
```

## 허용과 차단

| 조건 | 처리 |
| --- | --- |
| 활성 owner/operator, FAILED Run/Work, RETRY_EXHAUSTED, cancel epoch 0, 도구 전송 이력 없음 | 현재 mock 접수 정책을 재검증하고 새 Run 생성 |
| OUTCOME_UNKNOWN 또는 Tool dispatch 이력 존재 | 409; provider 결과 조정으로 이동 |
| 취소·성공·진행 중 Run, 일반 입력 검증 실패, 지원하지 않는 provider | 재실행 거부 |
| 다른 Tenant/Project, Membership 철회 또는 비활성 Principal | 404; 존재 여부를 노출하지 않음 |
| 같은 DLQ에 같은 Principal/key/reason 재요청 | 기존 자식 Run 반환, duplicate=true |
| 이미 redrive한 DLQ에 다른 요청 | 409; 두 번째 자식을 만들지 않음 |

자식이 다시 실패하면 그 자식의 DLQ를 별도로 조사한다. 자동 반복·일괄 redrive는 없다. `WORKER_RECOVERED`, `TRANSIENT_FAILURE_RESOLVED`는 운영자의 제한된 사유 코드이며, 시스템이 장애 해결을 증명했다는 의미가 아니다. 운영자가 장애 원인을 먼저 확인해야 한다.

## API와 수동 운영 절차

1. GET Run과 events로 실패 원인·도구 전송·취소 여부를 확인한다. DLQ 조회는 raw 입력/결과 없이 ID·reason code·생성 시각·연결된 자식만 반환한다.

```bash
curl -sS 'http://127.0.0.1:8000/v1/runs/<run_id>/dead-letters?limit=100' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy'
```

조회는 ID 오름차순이다. 다음 페이지는 마지막 item의 `id`를 `after_id`로 전달한다. 시간순 정렬이나 snapshot pagination은 아니다. `limit`은 1–100이다.

2. UNKNOWN이면 기존 `/v1/runs/<run_id>/reconcile`로 provider의 저장 결과를 확인한다. 조회 결과가 없다고 실패로 강제 변경하거나 새로운 key로 도구를 호출하지 않는다.
3. 허용 대상이고 장애가 해소되었으면 고유한 Idempotency-Key로 명시적으로 redrive한다. key는 일반 Run 접수와 분리된 namespace이며 같은 Tenant/Project/Principal 안에서 다른 redrive 요청에 재사용하지 않는다.

```bash
curl -sS -X POST \
  'http://127.0.0.1:8000/v1/runs/<run_id>/dead-letters/<item_id>/redrive' \
  -H 'Authorization: Bearer local-demo-token-do-not-deploy' \
  -H 'Idempotency-Key: incident-001-redrive-1' \
  -H 'Content-Type: application/json' \
  -d '{"reason":"WORKER_RECOVERED"}'
```

입력·Agent 버전·Tenant·Project·강제 실행 옵션·외부 성공 증거를 body로 지정할 수 없다. body의 사유 코드와 헤더 key는 필수다. 응답의 `202`는 접수만 의미하며 실행 성공이 아니다. 응답의 새 `run_id`로 결과·event를 관측한다. 응답을 놓쳤다면 **같은 key와 사유**로 재요청한다.

## 원자성·감사·권한 경계

- 기존 Work→Run→Step→Effect 잠금 순서를 유지한다. 원본 상태와 전송 이력을 확인한 transaction 안에서 자식과 연결을 저장한다.
- source DLQ당 연결 하나, 자식 Run당 연결 하나, 요청 key scope의 unique 제약으로 동시 요청을 중복 생성하지 않는다.
- 새 redrive 기록은 append-only이며 Tenant RLS와 Project 소유권 composite FK를 가진다. HTTP 경로의 Run과 DLQ 소속도 검증한다.
- 원본의 실패 상태는 유지하되 redrive 감사 event를 추가하면서 `state_version`을 증가시킨다. 자식에도 source 연결 event를 남긴다. 사유는 allowlist 코드이며 입력·provider 결과를 event에 복사하지 않는다.
- API는 현재 Membership을 확인하고 worker는 실행 시 자식 Principal 권한을 다시 확인한다. 운영 identity provider, 전용 세분화된 redrive permission, 승인 워크플로는 아직 없다.

## Migration과 한계

`0004`는 새 연결 테이블과 제약을 추가하며 기존 0001–0003 migration과 기존 DLQ 이력을 수정하지 않는다. API/worker를 중지한 로컬 유지보수 시간에 기존 [migration 경계](phase-2b.md#migration과-운영-제약)를 확인하고 upgrade한다. 기존 설치에 이전 migration이 남아 있다면 해당 guard도 적용된다.

downgrade는 redrive 기록이 있으면 거부한다. 운영 이력을 지워 버전만 되돌리는 것은 지원하지 않는다. 이 작업의 migration 테스트는 disposable PostgreSQL에서만 실행하며 사용자 DB에는 적용하지 않는다.

실제 외부 provider, 동일 Run의 checkpoint redrive, pause/resume, 자동 redrive, 운영용 RBAC·rate limit·quota는 범위 밖이다. 같은 DB를 사용하는 mock provider의 보장 범위는 [Phase 2b](phase-2b.md)를 따른다. 다음 단계는 run/step/model/tool trace와 평가 데이터 연결이다.

## 검증 결과

2026-09-13 로컬 전체 **229 tests passed**. Ruff format/lint, Pyright, frozen dependency sync, wheel/source build, Compose config와 diff 검사가 통과했다. 신규 31개에는 API 계약, 실제 PostgreSQL 동시 요청·rollback·RLS·migration, API→새 Run→worker 완료 흐름이 포함된다. 독립 read-only 리뷰에서도 확정적 결함을 발견하지 못했다. 운영 배포 또는 실 provider 장애 복구를 검증한 결과는 아니다.
