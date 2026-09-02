# Agent Runtime Platform

에이전트의 정의부터 실행, 도구 호출, 메모리, 복구, 추적까지 담당하는 엔터프라이즈급 런타임입니다.

> 현재 상태: 구현 전 기준 설계와 첫 번째 AI 업무 예제를 정의한 초기 저장소

## 설계 문서

- [Agent Runtime Platform 엔터프라이즈 설계](docs/architecture/agent-runtime-platform.md)
- [AI 모델 릴리스 Agent 예제 패키지](examples/ai-model-release/README.md)

## 목표

- 서버나 워커 장애 후에도 중단된 실행을 안전하게 이어갑니다.
- provider의 idempotency 계약 안에서 도구 중복 효과를 방지하고, 결과가 불명확하면 자동 재시도 대신 조정 절차로 전환합니다.
- 모든 실행을 `run → step → model call → tool call → result` 단위로 추적합니다.
- Tenant와 Project별 권한, 한도, 비용, 데이터 보존 정책을 분리합니다.

## 책임 범위

| 영역 | 책임 |
| --- | --- |
| Control Plane | Tenant·Project·Membership·Connection과 Agent·Tool 불변 버전 |
| Runtime | 상태 전이, 순차 스텝 스케줄링, 중단·재개, 장애 복구 |
| Tool Gateway | 스키마 검증, 인증, 승인, timeout, 감사 로그 |
| Memory | 단기·장기 메모리, 출처, TTL, 사용자·Project·Tenant 격리 |
| Observability | trace 연결, 실패 재현, 민감정보 마스킹 |
| Governance | quota, rate limit, 실행 예산, 무한 루프 방지 |

플랫폼 코어는 특정 업무를 알지 못합니다. 모델 평가·벤치마크·배포 같은 AI 업무는 Agent Definition과 Tool 패키지로 등록하며, 해당 패키지가 없어도 플랫폼은 정상적으로 실행되어야 합니다.

## 핵심 지표

- 실행 성공률 및 장애 후 복구율
- 중복 도구 실행률
- P95 실행 지연시간
- trace 누락률
- 실행당 토큰·도구 비용

## 업무 백로그

### 1. 실행 코어

- [ ] 실행 상태 머신과 전이 규칙 정의
- [ ] PostgreSQL 기반 event/run store 설계
- [ ] worker lease, heartbeat, 작업 인계 구현
- [ ] streaming, cancellation, backpressure 처리

### 2. Durable Execution

- [ ] checkpoint와 resume 구현
- [ ] retry, exponential backoff, dead-letter queue 구현
- [ ] idempotency key와 도구 실행 원장 설계
- [ ] 외부 부작용 실패 시 조회·보상 전략 작성

### 3. Tool Gateway와 보안

- [ ] JSON Schema 기반 입력·출력 검증
- [ ] 도구별 권한 scope와 human approval 구현
- [ ] secret 전달 및 외부 네트워크 정책 정의
- [ ] sandbox 실행과 감사 로그 구현

### 4. Memory와 멀티테넌시

- [ ] Tenant·Project·Principal·Membership 권한 모델 구현
- [ ] Connection metadata와 secret reference 수명주기 구현
- [ ] 사용자·조직별 메모리 격리
- [ ] 출처, 갱신 시점, TTL, 삭제 정책 구현
- [ ] tenant별 quota, rate limit, 비용 집계

### 5. 추적과 품질 운영

- [ ] OpenTelemetry 기반 run trace 연결
- [ ] prompt/model/tool 버전 기록
- [ ] redaction, sampling, retention 정책 구현
- [ ] 실패 trace를 회귀 평가 데이터셋으로 전환

## 마일스톤

| 단계 | 결과물 |
| --- | --- |
| M1 | 단일 워커에서 실행·도구 호출·상태 저장 |
| M2 | 워커 강제 종료 후 복구 및 중복 실행 방지 |
| M3 | trace viewer, 취소, timeout, DLQ |
| M4 | 멀티테넌시, 비용·quota, 보안 정책 |
| M5 | 실패 재실행과 배포 전 회귀 평가 |

## 완료 기준

- 워커 강제 종료 시나리오에서 실행이 유실 없이 복구됩니다.
- idempotency를 지원하는 provider에서는 같은 effect가 중복 반영되지 않고, 지원하지 않는 provider의 불명확한 결과는 `OUTCOME_UNKNOWN`으로 격리됩니다.
- 하나의 run을 입력부터 결과까지 trace로 재구성할 수 있습니다.
- Tenant 간 격리와 동일 Tenant 내 Project 권한·비용 분리를 자동 테스트로 증명합니다.
