# Messaging & Distributed Systems Study Plan

> Kafka를 중심축으로 두되, managed streaming service · 전통적 message queue · 실제 case study ·
> distributed systems 이론을 함께 엮어 공부한다.
> 특히 Kafka producer / consumer를 실무에서 어떻게 구성할지를 하나의 축으로 꾸준히 다룬다.

## 기본 원칙

- **분량**: 주 2시간 목표, 3개월(13주)
  - 권장 배분: **이론 · 읽기 1시간 + 실무 · 실습 1시간**
- **세 개의 축**
  1. **DS 이론** — 개념과 보장(guarantee)을 이해
  2. **Messaging 시스템** — Kafka · RabbitMQ · managed service가 그 개념을 어떻게 구현하는지
  3. **Client 실무** — producer / consumer 설정 · 구조 · 에러 처리를 어떻게 설계하는지
- **넓고 얕게, 핵심은 깊게**: 책은 챕터별 핵심 절만 골라 읽고,
  client 실무 · replication · delivery semantics에 시간을 가장 많이 쓴다
- **책은 지도일 뿐**: 아래 챕터 매핑은 출발점이며, 흥미가 생기는 방향으로 자유롭게 벗어나도 됨
- **매주 산출물**: 짧은 노트 1개 (핵심 키워드, 이해한 것, 남은 질문) + 실습 코드/설정 변경분

## 러닝 실습 프로젝트

13주 동안 작은 producer / consumer 애플리케이션 하나를 점진적으로 키워 나간다.

- 소재 예시: 주문 이벤트 파이프라인 (주문 생성 → 결제 → 알림), 클릭 로그 수집 등 자유
- 언어 · 프레임워크 자유: Java client, Spring for Apache Kafka, librdkafka 기반 client(Go/Python 등)
- 환경: 로컬 Docker(KRaft 모드), 테스트는 Testcontainers 등
- 각 Phase의 "Client 실무" 항목을 이 프로젝트에 적용해 보는 것이 실습의 기본 형태

## 참고 자료

### 주교재

- **[DS]** Maarten van Steen & Andrew S. Tanenbaum, *Distributed Systems* (4th ed.)
- **[KDG]** Gwen Shapira et al., *Kafka: The Definitive Guide* (2nd ed.)

### 보조 자료 (필요할 때 골라서)

- Martin Kleppmann, *Designing Data-Intensive Applications* — 이론과 실무의 다리 역할
- Alex Petrov, *Database Internals* — replication · consensus 파트
- Ben Stopford, *Designing Event-Driven Systems*
- Gavin Roy, *RabbitMQ in Depth*
- Jay Kreps, "The Log: What every software engineer should know about real-time data's unifying abstraction"
- 논문: Lamport (Time, Clocks), FLP, Raft, LinkedIn Kafka 논문(2011)
- Kafka 공식 문서의 producer / consumer configs, KIP 문서 (KIP-848 등)
- Confluent 블로그 · 개발자 문서, Jepsen 분석 글, 각 클라우드 공식 문서, 기업 엔지니어링 블로그

---

## 한눈에 보기

| 주차 | Phase | 이론 축 | Messaging · Client 실무 축 |
| --- | --- | --- | --- |
| 1 | Orientation | DS의 목표, 아키텍처 스타일 | queue vs log vs pub/sub, 실습 환경 구성 |
| 2 | Communication | RPC, MOM | RabbitMQ와 Kafka 모델 비교, topic 설계 |
| 3–5 | **Producer & Consumer 실무** | — | producer 설계, consumer 설계, 에러 처리 |
| 6–7 | Coordination | logical clock, Raft, FLP | KRaft, consumer group protocol · rebalance |
| 8–10 | Replication | consistency model, CAP | ISR, high watermark, 신뢰성 설정, exactly-once |
| 11 | Managed & 대안 | — | Kinesis, Pub/Sub, MSK, diskless Kafka, client 차이 |
| 12 | Ecosystem & Case | — | outbox · CDC 패턴, 기업 사례 |
| 13 | Wrap-up | — | producer / consumer 설계 문서(캡스톤) |

---

## Phase 1. Orientation — 왜 메시징인가 (Week 1)

| 영역 | 키워드 |
| --- | --- |
| DS 이론 | distributed system의 목표, Fallacies of Distributed Computing, architectural styles (event-based, publish-subscribe) |
| Messaging | 동기 vs 비동기, decoupling, queue vs log vs pub/sub, "log as a unifying abstraction" |
| Client 실무 | 로컬 Kafka(KRaft) 환경, CLI produce/consume, 러닝 프로젝트 소재 정하기 |

- 참고: [DS] Ch.1–2 훑어보기, [KDG] Ch.1–2, Jay Kreps "The Log"

## Phase 2. Communication & Message Queue (Week 2)

| 영역 | 키워드 |
| --- | --- |
| DS 이론 | RPC, message-oriented communication, transient vs persistent, MOM |
| 전통적 MQ | AMQP 모델 (exchange · queue · binding), RabbitMQ ack/prefetch, dead-letter queue |
| 비교 관점 | smart broker vs smart consumer, 메시지 삭제 vs retention, replay, push vs pull |
| Client 실무 | topic 설계: partition 수 산정, naming 규칙, retention vs compaction, 이벤트 스키마 초안 |

- 참고: [DS] Ch.4 중 message-oriented communication, *RabbitMQ in Depth* 개요
- 확장 키워드: Queues for Kafka(share group, KIP-932)

## Phase 3. Producer & Consumer 실무 (Week 3–5)

이 플랜에서 가장 실무 비중이 높은 Phase. 주차별로 하나의 주제에 집중한다.

### Week 3 — Producer 설계

| 영역 | 키워드 |
| --- | --- |
| Key · partitioning | key 설계와 ordering 보장 범위, hot partition, custom partitioner, sticky partitioner |
| 전송 모델 | sync vs async send, callback, producer instance 공유(thread-safe), lifecycle · graceful close |
| 처리량 · 지연 | `batch.size`, `linger.ms`, compression, `buffer.memory`와 backpressure(`max.block.ms`) |
| 신뢰성 기본값 | `acks`, `retries`, `delivery.timeout.ms`, `max.in.flight.requests.per.connection`, idempotence(기본 활성) |
| 직렬화 | Avro / Protobuf / JSON Schema, Schema Registry, schema evolution과 compatibility 모드, headers 활용 |

- 참고: [KDG] Ch.3

### Week 4 — Consumer 설계

| 영역 | 키워드 |
| --- | --- |
| Group 구성 | partition 수 vs consumer 수, `group.id` 전략, `auto.offset.reset` |
| Poll loop | `max.poll.records`, `max.poll.interval.ms`, `session.timeout.ms`, heartbeat, fetch 튜닝 |
| Offset 관리 | auto commit vs manual commit(sync/async), commit 시점과 at-least-once, seek · replay |
| 동시성 모델 | consumer는 thread-safe하지 않음, thread-per-consumer vs worker pool, ordering vs parallelism, key 단위 병렬 처리(parallel consumer류) |
| 흐름 제어 | pause/resume, 느린 downstream 대응, lag 모니터링 |

- 참고: [KDG] Ch.4

### Week 5 — 에러 처리 & 운영 관점

| 영역 | 키워드 |
| --- | --- |
| Producer 에러 | retriable vs non-retriable error, timeout 이후 상태 불명확성, 중복 발생 지점 |
| Consumer 에러 | poison pill, deserialization error, retry 전략(in-place / retry topic / backoff), DLQ |
| 멱등 처리 | consumer 측 idempotency (dedup key, upsert, processed-offset 저장) |
| 관측 · 테스트 | client metrics, consumer lag 알림, Testcontainers 기반 통합 테스트, 장애 주입(브로커 재시작, 네트워크 지연) |
| 프레임워크 | Spring Kafka 등 프레임워크가 제공하는 error handler · retry · container 설정과 raw client의 차이 |

- 참고: [KDG] Ch.4 후반, Confluent 블로그의 error handling · retry 관련 글

## Phase 4. Time, Coordination, Consensus (Week 6–7)

| 영역 | 키워드 |
| --- | --- |
| DS 이론 | logical clock (Lamport, vector clock), happened-before, leader election |
| Consensus | FLP impossibility, Raft (Paxos는 개념만), quorum, split brain, fencing / epoch |
| Kafka | controller, ZooKeeper → KRaft 전환(KIP-500), metadata log, partition leader election |
| Client 실무 | group coordinator와 rebalance 동작, eager vs cooperative rebalance, static membership(`group.instance.id`), 새 consumer group protocol(KIP-848), rebalance listener에서의 commit · 상태 정리 |

- 참고: [DS] Ch.5 일부 + Ch.8 consensus 절, [KDG] Ch.6, Raft 논문(또는 Raft 시각화 자료)

## Phase 5. Replication, Consistency, Delivery Semantics (Week 8–10)

| 영역 | 키워드 |
| --- | --- |
| DS 이론 | linearizability, sequential/causal/eventual consistency, primary-backup vs quorum replication, failure model, CAP / PACELC |
| Kafka 복제 | ISR, high watermark, leader epoch, unclean leader election, `min.insync.replicas` |
| Delivery semantics | at-most/at-least/exactly-once, idempotent producer 내부 동작(PID, sequence number), transactions, `isolation.level` |
| Client 실무 | 신뢰성 설정 조합표(`acks` × `min.insync.replicas` × replication factor), transactional consume-process-produce 구현, 외부 DB와 함께 쓸 때 exactly-once가 깨지는 지점, 장애 시나리오별 데이터 유실 · 중복 실험 |
| 검증 관점 | Jepsen식 사고: "무엇을 보장한다고 주장하고, 실제로는 어떤가?" |

- 참고: [DS] Ch.7–8 핵심 절, [KDG] Ch.7–8, DDIA replication 챕터, Jepsen Kafka 분석
- 이론과 실무가 가장 강하게 만나는 Phase이므로 3주를 배정

## Phase 6. Managed Services & 대안 설계 (Week 11)

| 영역 | 키워드 |
| --- | --- |
| AWS | Kinesis Data Streams (shard, KCL, enhanced fan-out, on-demand), SQS/SNS, MSK |
| Google Cloud | Pub/Sub (push/pull, ack deadline, ordering key, exactly-once delivery) |
| 대안 아키텍처 | Pulsar, Redpanda, tiered storage(KIP-405), diskless Kafka(WarpStream, KIP-1150) |
| Client 실무 | 서비스별 client 모델 비교: KCL의 checkpoint · lease, Pub/Sub의 ack deadline · flow control, MSK 인증(IAM) 설정 |
| 정리 산출물 | 비교 매트릭스: ordering 단위 · 보장 수준 · retention · replay · scaling 단위 · 비용 구조 |

- 참고: 각 서비스 공식 문서와 아키텍처 백서

## Phase 7. Ecosystem, Patterns & Case Studies (Week 12)

| 영역 | 키워드 |
| --- | --- |
| 설계 패턴 | transactional outbox, CDC(Debezium), event sourcing, CQRS, log compaction 활용 |
| Kafka 생태계 | Kafka Connect, Schema Registry 운영, Kafka Streams · Flink 개요 |
| Case studies | LinkedIn · Uber · Netflix 중 1개 + 국내 사례 1개, 특히 client 측 설계(consumer proxy, retry 구조 등)에 주목 |

- 참고: [KDG] Ch.9, Ch.14 훑어보기, *Designing Event-Driven Systems*

## Phase 8. Wrap-up (Week 13)

- 3개월간의 노트 회고, 비교 매트릭스 보완
- 캡스톤: 러닝 프로젝트를 기준으로 **producer / consumer 설계 문서** 작성
  - topic · key · partition 설계, 신뢰성 설정과 근거, consumer 동시성 모델,
    에러 처리 · DLQ 전략, 모니터링 지표, 대안 기술(managed service 등) 대비 선택 이유

---

## 3개월 이후 / 여력이 될 때

이번 플랜에서 덜어낸 주제들. 진도가 빠르거나 후속 스터디를 할 때 사용:

- Kafka 운영 · 보안: SASL/mTLS/ACL, partition 재배치, capacity planning ([KDG] Ch.10–13)
- DS 이론 보강: naming, 분산 시스템 보안 ([DS] Ch.6, Ch.9), distributed snapshot (Chandy-Lamport)
- 분산 트랜잭션: 2PC, saga, 그리고 왜 메시징 시스템은 2PC를 피하려 하는가
- Stream processing 심화: event time, watermark, windowing, stateful processing
- 기타 시스템: NATS JetStream, Azure Event Hubs, Confluent Cloud
- CRDT, Byzantine fault tolerance, TLA+로 프로토콜 모델링
- Kafka 소스 코드 읽기, 성능(zero-copy, page cache, sequential I/O)
