# Phase 3c: 명시적으로 정제한 case와 부작용 없는 회귀 평가

## 목표와 경계

DRAFT candidate의 출처 digest를 확인하고 검토자가 직접 제공한 정제 입력·기대 ToolInvocation을 별도 불변 case로 저장한다. 원본 candidate는 DRAFT 그대로이며 원문 trace에서 입력을 자동 추출하지 않는다. 현재 권한 확인 + 명시적 review_confirmed는 제출자의 검토 확인일 뿐 독립 승인·PII 자동 검증을 뜻하지 않는다.

case ID 목록(1~50개, 중복 금지, 한 Project)을 content digest로 고정한 suite manifest로 만들고 mock ModelGateway 판단만 실행한다. exact ToolInvocation scorer v1으로 PASS/MISMATCH/INVALID_OUTPUT/PROVIDER_ERROR/TIMED_OUT을 집계한다. ToolGateway·RuntimeKernel·worker queue를 호출하지 않는다. 기존 effect/Run 상태를 바꾸지 않으며 UNKNOWN을 안전 재실행으로 간주하지 않는다.

보고서는 동기 응답의 metadata-only JSON이다. case 목록과 digest, model/scorer revision을 포함한다. input/output/예외 원문은 보고서에 넣지 않는다. durable 평가 job/report 저장소·실 LLM·품질점수·자동 release gate·dataset 승인/승격은 후속이다. 현재 유일한 실행 route는 mock/release-planner-v1이다.

## 구현

1. Root: application/evaluation.py의 불변 case/manifest/report 계약과 순수 runner, 제한·검증·단위 테스트.
2. DB agent: adapters/postgres/evaluations.py, migration 0006 및 실제 PG 테스트. source candidate FK, tenant RLS, Project/membership 확인, scoped idempotency, source/content digest, append-only case.
3. Root: API POST candidate/{id}/cases, GET evaluation-cases/{id}, POST evaluations/offline. composition/입력 제한/권한 재확인, end-to-end 테스트·문서.
4. 독립 리뷰와 전체 regression/frozen sync/lint/type/build/Compose 검증 후 main push/CI 확인.

## 검증/운영

- 정제 확인 누락, source digest mismatch, 다른 tenant/project/권한 철회, 같은 키 다른 content, 동시 생성, immutable DB trigger/FK/RLS.
- deterministic manifest, 입력 변조 방어, 정확한 도구명·인자 비교, 오류·timeout·취소 보존, report에 원문 canary 없음, unsupported route/no Tool execution.
- 기존 candidate·Run/event/effect 변경 없음; migration은 disposable PostgreSQL에서만. 기존 사용자 DB 업그레이드/운영 배포는 하지 않는다. case 존재 시 downgrade는 거부한다.
- 최종 `uv sync --frozen`, Ruff, Pyright, 전체 pytest, build, Compose config, diff, remote SHA 및 해당 CI 실행 확인.

## 구현/검증 결과

- 별도 불변 case 저장소·migration 0006, 세 API, model-only exact scorer와 manifest/report 구현.
- caller 데이터 복사·JSON/NUL/크기 제한, JSONB 숫자 정규화·idempotency/무결성 회귀를 추가했다. 기본 role의 기존 권한을 넓히지 않았으며 report 반환 전 현재 접근권한을 재확인한다.
- 전체 426개 테스트, frozen sync/lint/type/build/Compose/diff 통과. 독립 재리뷰의 72개 집중 테스트에서 추가 차단 문제 없음.
- [구현 현황과 사용법](../../implementation/phase-3c.md). report는 비영속 응답이며 source DRAFT/Run/Tool 효과는 변경하지 않는다. 사용자 DB/운영 환경에는 적용하지 않는다.
