# Phase 3b: Worker OpenTelemetry 관측

Phase 3a의 PostgreSQL trace 조회와 별도로, 실제 worker 실행 중 OTel SDK span을 생성하고 로컬 OTLP/HTTP 수신기로 전송한다. 기본값은 비활성화이며 실행 상태·복구·평가 후보 계약은 바꾸지 않는다. 새 migration은 없다.

## 실행과 관측의 관계

```text
PostgreSQL claim → RuntimeKernel.execute → 결과/실패/재시도 처리 commit
                         │
                         └─ Attempt root span
                              └─ Model 또는 Tool gateway child span
                                      │ span 종료: 메모리 큐에 enqueue
                                      ▼
                            bounded queue → daemon exporter → loopback OTLP receiver
```

각 물리 Attempt는 독립적인 trace다. 정상 Model Step과 Tool Step은 두 개의 trace, 네 개의 span을 만든다. 동일한 `runtime.run.id`와 Step/Attempt ID로 DB 이력에 연결한다. 아직 API→queue context 전달, 전체 Run root span, 재시도 간 span link, reaper/reconcile/API span은 없다.

Attempt span은 Kernel 진입부터 저장소 호출 반환까지의 시간이다. DB claim·대기·heartbeat는 포함하지 않는다. child span은 gateway await 구간이며 결과 schema 검증·commit은 포함하지 않는다. 현재 gateway는 mock이므로 실제 LLM의 TTFT·TPOT·token·비용 측정이 아니다.

### Outcome은 Run 상태와 다르다

| span outcome | 의미 |
| --- | --- |
| `RETURNED` | gateway가 응답했다. schema 적합성·효과 확정·Run 성공을 뜻하지 않는다. |
| `RECORDED` | 완료 처리 저장소 호출이 정상 반환했다. Model은 다음 Tool Work만 예약할 수 있고 Tool 완료는 취소 요청과 함께 처리될 수 있다. |
| `RETRY_HANDLED` | 재시도 정책 처리가 commit됐다. 실제 결과는 재예약 또는 한도 소진에 따른 FAILED/DLQ 등이며 DB를 조회해야 한다. |
| `OUTCOME_UNKNOWN` | dispatch 이후 효과가 불명확한 처리 경로다. 재시도를 허용하는 증거가 아니다. |
| `POLICY_DENIED`, `CLIENT_INVALID`, `FAILED` | Kernel이 해당 실패 처리를 저장했다. |
| `TIMED_OUT`, `CONFLICT`, `CANCELLED`, `ERROR` | 해당 실행/예외 경로가 관측됐다. 특히 persistence timeout·commit 오류·task 취소만으로 durable Run 상태를 확정하지 않는다. |

관측 코드의 start/end 오류는 원래 반환값·예외·`CancelledError`를 바꾸지 않는다. Tool span은 dispatch 승인 transaction 이후에만 시작한다. 최종 진실은 항상 PostgreSQL의 Run/Attempt/Effect이며, span 성공·부재로 실행 성공·유실을 판정하지 않는다.

## 개인정보와 전송 경계

- `runtime.*` 자체 mapping v1을 사용한다. 변화할 수 있는 GenAI semantic convention과 DB schema를 결합하지 않는다.
- 고정 operation/outcome, attempt number, Run/Step/Attempt/Agent·Tool version의 opaque ID만 전송한다. 32/48/64자리 소문자 hex와 UUID 형식 외의 ID는 SHA-256 hash로 치환한다.
- Tenant/Project는 SHA-256 hash만 전송한다. 이는 익명화나 접근 통제가 아니다. 값이 추측 가능하면 사전 대입이 가능하고, hash/ID 모두 연결 가능한 metadata이므로 collector 접근·보존 정책이 필요하다.
- input/output, prompt/spec, Tool 인자·결과, error message/stack, principal/worker ID, provider key/dispatch token을 전송하지 않는다. ID 자체에 비밀을 인코딩해 넣는 사용은 지원하지 않는다.
- 명시적 Resource와 local TracerProvider를 사용한다. 자동 instrumentation, global tracer provider 설정, env resource detector, ambient baggage/parent 전파는 없다. SDK 내부 metrics도 명시적인 NoOp provider로 격리한다.
- `OTEL_SDK_DISABLED=true`는 SDK 정책대로 span을 끈다. 잘못된 SDK internal-metrics 설정은 원문을 로그에 남기지 않고 telemetry 초기화를 거부하며 worker는 Noop으로 계속 실행한다.
- HTTP literal loopback IP의 `/v1/traces`만 허용한다. `localhost` 등 DNS 이름, 외부 주소, credential, query, fragment, IPv6 scope/mapped 주소는 거부한다. 환경 proxy/OTLP headers, redirects와 transport retries는 사용하지 않는다.
- 공식 OTel SDK·OTLP protobuf encoder를 사용하되 transport는 제한된 `http.client` 구현이다. SDK 기본 HTTP exporter의 환경 설정과 응답 본문 로그를 끌어오지 않는다. 부분 거부·비정상 응답·oversize protobuf는 실패로 계수하고 본문은 기록하지 않는다.

이는 운영용 TLS·인증 collector 구성이 아니다. 로컬 receiver의 이후 forwarding, 인증·보존·삭제 정책은 이 코드가 통제하지 않는다. container의 loopback은 해당 container 자체이며 Compose service DNS 연결은 이 단계에서 지원하지 않는다. Python worker는 기존 CLI처럼 독립 프로세스로 시작하고 telemetry 초기화 후 fork하여 재사용하지 않는다.

## 비차단·유실 정책

`on_end`는 짧은 lock 아래 bounded deque에 넣기만 한다. 네트워크는 하나의 daemon thread에서 span 한 개씩 전송한다. 기본 capacity 2048에 in-flight 최대 1개가 추가된다. 큐가 차면 새 span을 버리며 실행에 backpressure를 전달하지 않는다. exporter 실패는 재시도하지 않는다.

소켓 timeout은 개별 I/O 대기 제한이며 전체 전송의 wall-clock deadline은 아니다. 응답을 조금씩 보내거나 주입된 exporter가 멈추면 단일 전송 스레드가 오래 점유될 수 있다. 그래도 큐 크기와 worker 종료 대기는 제한된다. 외부 스레드를 강제로 종료하지 않으며, 미완료 전송은 `in_flight`로 남는다.

종료 시 새 span 접수를 중단하고 설정된 시간만 drain을 기다린다. 남은 큐는 drop하며, stuck exporter의 강제 종료나 전송 성공을 보장하지 않는다. SIGKILL/crash 시 아직 전송하지 않은 span과 메모리 counters는 유실된다. durable outbox·재전송·exactly-once telemetry는 아니다.

### Counters

worker loop는 약 30초마다, 그리고 종료 시 고정 key와 정수 counters만 로그에 남긴다. 긴 작업 중에는 다음 loop까지 로그가 지연될 수 있다.

| key | 범위 |
| --- | --- |
| `ended` | processor가 받은 종료 span |
| `enqueued` | 큐에 수용한 누적 span |
| `exported` / `failed` | 성공/실패로 끝난 전송의 span 수 |
| `dropped` | queue full·종료로 버린 span 수 |
| `queue_depth` / `in_flight` | 현재 대기/전송 중 span 수 |
| `instrumentation_errors` | adapter가 포착한 계측·exporter 종료 오류 수 |

`force_flush`의 true는 큐와 in-flight가 비었다는 뜻이지 전송이 모두 성공했다는 뜻이 아니다. 실패/drop도 함께 확인한다. 계측 시작 전 실패 등은 안전한 고정 warning으로 남을 수 있다. 이 counters는 process-local 진단이며 Prometheus/OTel metrics export·fleet 집계가 아니다.

## 로컬 사용

기본 worker 명령은 변경 없이 관측 비활성 상태로 실행된다. 별도로 마련한 로컬 OTLP/HTTP receiver가 있을 때만 켠다. 저장소가 collector를 자동 설치·실행하거나 외부에 전송하지 않는다.

```bash
AGENT_PLATFORM_TELEMETRY_ENABLED=true \
AGENT_PLATFORM_TELEMETRY_ENDPOINT=http://127.0.0.1:4318/v1/traces \
uv run python -m agent_platform.worker.main
```

선택 설정은 `.env.example`을 따른다. queue capacity는 1~10000, export socket timeout은 0초 초과~10초, shutdown budget은 0초 초과~10초다. 기본값은 각각 2048, 1초, 2초다. 다른 설정과 마찬가지로 잘못된 URL·범위는 시작 전 validation error다. 유효한 설정의 SDK 초기화 실패는 Noop으로 대체한다.

## 검증과 후속 범위

실제 disposable PostgreSQL + worker drain + 로컬 HTTP protobuf 수신 테스트로 2-Step 완료, span 연결·원문 배제, 수신기 503에도 결과 보존을 검사한다. 별도 blocked exporter/queue full 테스트, retry exhaustion의 DB 상태와 span 표현 회귀 테스트, SDK context/환경 격리·부분 응답·shutdown 테스트를 포함한다.

2026-09-14 로컬 전체 **353 tests passed**(기존 272개 + 신규 81개). Ruff format/lint, Pyright, frozen dependency sync, source/wheel build, Compose config와 diff 검사 통과. 독립 리뷰에서 retry exhaustion을 예약으로 오인하는 span 표현과 SDK 전역 metrics 경계를 발견해 수정하고 회귀 테스트를 추가했다. 재리뷰와 집중 100개 단위 테스트에서 추가 차단 문제를 발견하지 못했다. 사용자 DB migration·실제 외부 provider·운영 collector 배포를 수행한 결과는 아니다.

다음 범위는 DRAFT 후보의 검토·정제 입력·expected result·scorer 계약을 정의하고 버전 고정 offline 회귀 평가로 연결하는 것이다. Phase 3a 후보는 여전히 자동 평가·승인된 데이터셋이 아니다. full Run context, sampling/retention, 운영 TLS/auth collector, 실제 provider model/token/cost 계측도 별도 후속 범위다.

## 참고

- [OpenTelemetry Python 수동 계측](https://opentelemetry.io/docs/languages/python/instrumentation/)
- [OpenTelemetry Python SpanProcessor/Exporter API](https://opentelemetry-python.readthedocs.io/en/stable/sdk/trace.export.html)
- [공식 OTLP HTTP exporter 구현](https://opentelemetry-python.readthedocs.io/en/latest/_modules/opentelemetry/exporter/otlp/proto/http/trace_exporter.html)
