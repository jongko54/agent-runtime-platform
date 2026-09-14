# Phase 3b: 비차단 worker OpenTelemetry 계측

## 범위/설계

- 실제 실행 중 worker Attempt root span → Model/Tool gateway child span을 만든다. 각 Attempt는 별도 trace이며 durable Run/Step/Attempt ID로 연결한다. 아직 HTTP API→queue trace context 전달이나 전체 Run root span은 없다.
- 별도 telemetry port/adapter로 SDK를 분리한다. SDK provider는 local instance이며 global provider, 자동 instrumentation, env resource detector, exception 자동기록과 baggage 전파를 사용하지 않는다.
- 허용 metadata: opaque Run/Step/Attempt/Agent/Tool version ID, attempt number, tenant/project hash, 고정 operation/outcome/code. 원문 input/output/spec/error/stack/worker identity/provider key·token은 금지한다.
- enabled=false가 기본. 명시적으로 켤 때도 HTTP loopback IP 수신기의 /v1/traces만 허용한다. HTTP proxy/redirect/env exporter headers·credentials를 사용하지 않는다. 실제 외부 운영 collector/TLS/auth는 후속 작업이다.
- official OTel SDK와 OTLP protobuf encoding을 사용하고 bounded queue/daemon export worker로 런타임 스레드에서 네트워크 I/O를 하지 않는다. queue full은 drop, export 실패는 유한 처리, shutdown은 제한된 대기만 허용한다. arbitrary hung exporter를 강제로 중단할 수는 없으며 그 경우 daemon thread와 유실을 명시한다.
- metrics는 프로세스 내 enqueue/export/failure/drop/queue/inflight counters, worker의 주기적 안전 로그와 종료 로그로 확인한다. platform 집계 Prometheus/OTel metric export는 아직 아니다.
- 관측 start/end/transport 실패는 실행 결과·원본 예외·CancelledError를 바꾸지 않는다. span RETURNED/RECORDED와 durable Run 성공을 구분한다. Tool dispatch 전후 신뢰성 경계 유지.
- DB migration/사용자 DB/운영 인프라 변경 없음. 평가 DRAFT, dataset 승격·회귀 runner는 변경하지 않는다.

## 구현/소유권

1. root: 공통 telemetry port, 안전 observe wrapper, RuntimeKernel gateway/attempt instrumentation, Settings/worker 구성, E2E·문서.
2. telemetry agent: `adapters/telemetry/otel.py` bounded SpanProcessor, metadata mapping, OTLP loopback transport, lifecycle/counters tests.
3. 독립 리뷰: privacy/env/proxy/redirect, queue saturation/hung exporter, exceptions/cancellation/DB truth 보존.
4. 전체 기존272tests + 신규 실제 PG/로컬HTTP exporter 테스트, lint/type/build/Compose, main commit/push/CI 확인.

## 검증

- 정상 span parent/child IDs·duration·metadata, 2Attempt 간 context누출없음.
- raw exception/input/output/ambient resource/baggage canary 없는 실제 OTLP protobuf.
- bounded queue/drop/failure/shutdown과 느린/실패 exporter에도 worker 성공·lease복구·취소 semantics 유지.
- 기본 비활성화 no-network, 안전 endpoint validation, collector redirect/HTTP partial success 처리.

`uv run ruff format --check .`, `uv run ruff check .`, `uv run pyright`, `uv run pytest -q`, `uv build`, `docker compose config --quiet`, `git diff --check`.

## 실행 결과

- 공통 port·Kernel·worker 구성, bounded adapter, 로컬 protobuf/실제 PostgreSQL 테스트와 문서 구현.
- 리뷰 후 `RETRY_HANDLED`로 DB retry exhaustion과 span 표현을 구분하고 SDK 내부 metrics를 NoOp으로 격리했다. 잘못된 SDK 환경값의 원문 로그와 초기화 실패 시 thread 시작도 방지했다.
- 전체 353 tests 및 lint/type/build/frozen sync/Compose/diff 검증 통과. 독립 재리뷰에서 추가 차단 문제 없음.
- 구현 계약·운영 한계는 [Phase 3b 현황](../../implementation/phase-3b.md)을 따른다. 운영 collector·사용자 DB는 변경하지 않는다.
