# 14. ConsumerRebalanceListener

- 컨슈머에게 할당된 파티션에 변화가 생겼을 때 개발자가 원하는 액션이 실행되도록 하는 콜백 인터페이스
- 3가지 메서드 존재
  - onPartitionRevoked : 파티션 소유권을 잃기 직전에 호출
  - onPartitionAssigned : 파티션 소유권을 얻기 직전에 호출
  - onPartitionLost : 파티션이 이미 다른 컨슈머에게 넘어간 예외적인 경우(cluster는 제외했지만 Consumer는 모를 때)
    - ex) 컨슈머 세션 타임아웃으로 그룹에서 이미 제외된 상황일 때

## onPartitionRevoked : 파티션 소유권을 잃기 직전에 호출
- 리밸런싱 혹은 정상적으로 컨슈머가 그룹 탈퇴할 때
- manual commit으로 동작하는 경우, 이 콜백 메서드에서 offset commit을 수행해야 함
- Classic
  - Eager
    - 소유권 모두 포기 -> 기존 할당된 모든 파티션이 파라미터로 전달됨
    - 무조건 호출됨 -> 기존에 할당된 파티션이 없더라도 [] 가 전달
  - Cooperative
    - 소유권 일부 포기 -> 기존 할당된 파티션 중 소유권 이전할 파티션만 파라미터로 전달됨
    - 선택적으로 호출됨 -> 만약 포기하는 파티션이 없다면 호출 X
- Async는 Cooperative와 동일하게 동작

- 정상적인 그룹 탈퇴일 때는 Classic, Async 모두 별 차이 없음 -> 기존에 할당된 모든 파티션이 파라미터로 전달됨

## onPartitionAssigned : 파티션 소유권을 얻기 직전에 호출
- 리밸런싱 완료될 대 정확히 한번 호출됨(할당된 파티션이 없어도) 
- -> 무조건 한번 호출되므로 리밸런싱 이벤트라고 봐도 무방
- Classic
  - Eager
    - 할당된 모든 파티션이 전달
    - 할당된 파티션이 없어도 [] 전달
  - Cooperative
    - 기존에 없었던 할당된 파티션이 전달
    - 할당된 파티션이 없어도 [] 전달
- Async는 Cooperative와 동일

## onPartitionLost : 자신에게 소유권이 없다는 것을 뒤늦게 알게되었을 때
- 리밸런싱 발생 혹은 그룹 탈퇴 시 컨슈머가 비정상 상태에 있었던 경우 호출
- 컨슈머가 더이상 이 파티션의 소유자가 아니라는 사실을 뒤늦게 깨달았을 때
- 위 콜백 메서드에서는 offset commit() 코드를 실행하면 안됨
  - 이미 다른 컨슈머에게 할당된 파티션들의 offset을 commit 하면 안됨
  - default 동작이 onPartitionRevoked 위임이기 때문에 만약 Commit을 Revoked에 넣어놓았다면 문제 될 수 있음


