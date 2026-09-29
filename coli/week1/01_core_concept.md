
## 주요 문장
- 파티션 안에서는 순서가 보장되지만 파티션간에는 순서가 보장되지 않는다.
- Replication은 파티션 단위로 이어짐

## 주요 개념
![img.png](img.png)
- Kafka : 분산 이벤트 스트리밍 플랫폼
  - open-source
  - distributed : 여러 대 컴퓨터를 네트워크로 연결해 하나의 거대한 시스템처럼 동작
  - event : 소프트웨어 시스템에서 발생한 사실을 나타내는 '불변 데이터'
  - streaming : 이벤트가 데이터 소스에서 목적지까지 '끊임없이 실시간으로' 흐르는 상태 <-> Batch
  - platform : 데이터 파이프라인의 뼈대가 되는 중앙 허브

- Kafka Cluster : 카프카가 동작하는 프로세스(서버) 집합
  - 확장성 : 브로커 추가를 통한 수평확장 용이
  - 안전성 : 분산/복제를 통한 장애 허용성 확보
  
- Broker : Kafka Cluster를 구성하는 프로세스 각각
  - 운영환경에서는 서버와 프로세스가 1:1
  - 한 서버에 여러 프로세스를 띄워서 클러스터를 구성하는 것도 가능


## Topic & Partition & Offset
![img_1.png](img_1.png)
- Topic : 메시지가 분류되어 저장되는 주제/카테고리
  - ex) user_info_change : 사용자 정보 변경
  - 하나의 카프카에 여러개의 토픽을 만들 수 있음

- Partition : 병렬 처리를 위해 Topic은 여러 Partition으로 나눠짐
  - 메시지는 파티션 중 한 곳에 / 불변으로 / append 됨 
  - FIFO이지만 소비되는 즉시 삭제되진 않는 log 성격(retention 기간 동안 존재)
    - [중요] Topic 단위 FIFO가 아니라 Partition 단위로 FIFO가 보장됨
  - 하나의 메시지는 반드시 하나의 파티션에만 저장(복제 record는 제외)

- Offset : 파티션 안에 저장된 메시지들의 순서
  - 0부터 순차적으로 증가함(64bit)

- Producer : 애플리케이션에서 발생한 이벤트를 topic에 저장
  - partitioner : 이벤트가 어떤 partition에 저장되어야 하는지를 결정

- Consumer : topic에 저장된 이벤트를 consume하여 목적에 맞게 처리
  - offset을 통해 어디까지 consume 했는지 추적 ex) 내가 A파티션에서 3번까지 읽었으니까 다음엔 4번 읽어야지

![img_2.png](img_2.png)
- Consumer Group : 동일한 목적으로 묶은 Consumer 그룹
  - 같은 group에 속한 consumer들은 partition들을 나눠 맡아 메시지 처리
  - offset 어디까지 읽었는지 관리는 Consumer Group 단위로 이뤄짐
    - 특정 consumer가 죽어도 같은 그룹의 다른 consumer가 이어 처리
  - partition 수보다 consumer가 더 많으면 노는 consumer 발생 (파티션이 3개인데 그룹 내 consumer가 4이면 1개가 아무 일 안함)
  - 서로 다른 Consumer group은 같은 topic도 독립적으로 consume
    - 어떤 이벤트 A에 대해서 피드 처리를 맡은 CG A, 알림 처리를 맡은 CG B가 있으면 독립적으로 처리
  - 모든 Consuemr는 Group 지정이 필수적으로 필요함

## Event & Message & Record
- 같은 개념인데 강조하는 관점만 다름
- Event(비즈니스) vs Message(전달) vs Record(카프카 내무 관점)

## Log & Segment
![img_3.png](img_3.png)
- Log : 하나의 파티션에 순서대로 append되는 immutable한 메시지 흐름
- Segment : Log를 실제로 저장하기 위해 여러개 파일로 나눈 단위
- Segment 파일 구성 : .log, .index, .timeindex 세개 파일이 하나의 segment
  - .log : 실제 메시지, 기록대상인 메시지들 중 첫 메시지의 offset응로 네이밍됨 ex) 0 offset 부터 기록된 log 파일 -> 0000000.log
  - .index, .timeindex : 실제 메시지를 인덱싱하기 위한 파일
  
- Segment가 필요한 이유
  - log를 하나로 관리하면 파일 크기가 너무 커져서 관리 힘듦 -> 일정 크기나 시간 단위로 새로운 segment 생성
  - active segment(현재 쓰고 있는 segment)만 append 가능 (나머지 segment들은 immutable)
  - log는 segment 단위로 삭제되고 특정 시간이나 사이즈 이상일 때 가장 오래된 것부터 삭제됨

## Replication & Leader & Follower & ISR
![img_4.png](img_4.png)
- Replication : 각 partition 데이터를 여러 브로커에 복제 -> 가용성 확보
  - Replication은 파티션 단위로 이루어짐
  
- Leader : partition에 produce/consume 요청을 처리하는 브로커 (한 partition 당 하나만 존재)
- Follower : Leader로부터 partition을 복제해 저장하는 브로커

- Replica : Leader + Follower
- ISR(In-Sync-Replica) : 복제가 잘 되고 있어 Leader와 동기화 상태인 replica들
  - 리더 다운 시 새로운 Leader 선출 후보
- Replication factor : topic을 생성할 때 원본 포함 복제본을 몇개 둘 것인지 설정
  - 보통 3을 추천하여 broker 수보다 클 수 없음

![img_5.png](img_5.png)

## Kraft & Zookeeper
![img_6.png](img_6.png)
- Zookeeper : Kafka라는 분산시스템을 안정적이고 일관성있게 유지하는데 필요한 메타데이터 관리 코디네이터
  - Kafka 구축할 때마다 별도로 Zookeeper도 설정 및 띄워주어야 했음
  -> 운영 복잡성, 스케일링 등등 여러 이슈로 은퇴

- Kraft : Zookeeper의 역할을 Kafka cluster 내부에서 일부 노드가 수행
  - 더 이상 Zookeeper cluster를 따로 구축, 관리할 필요가 없어짐
