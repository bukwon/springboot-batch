- application.yml에서 url: jdbc:h2:mem:db_dev;MODE=MYSQL mem(메모리모드) 설정 안해주면 실행 x
- 배치 안에 여러 잡들이 존재 그리고 잡 안에는 스텝들이 있고 스텝 안엔 여러 태스클릿이 존재
- 스프링 배치의 핵심은 기록이다.
- 스프링 배치에서 작업은 각각의 독립적인 Job 으로 구성된다.
- 하나의 Job은 여러개의 Step들로 구성된다.
- 각 Step은 하나의 태스클릿이나 하나의 청크기반작업(클래스 3개)로 구성된다.
- 각 Step은 순서를 정할 수 있고, 병렬실행도 가능하다.
- 스프링 배치의 모든 실행시도는 기록된다.
- 특정 Job을 이전과 같은 인자를 입력하여 시도하면, 해당내용을 재시도로 인식한다.
- 재시도의 경우 기존에 성공이력이 있다면 실질적으로 일을 다시 수행하지 않고, 기존에 성공했던 작업이라서 이번에는 스킵한다는 기록만 추가된다.
- 스프랭 배치에서 각 Step 의 실행시도는 모두 기록된다.
- 해당 잡을 재시도할때에 실패한 Step 부터 다시 작업을 이어갈 수 있다.
- 특정한 Step을 골라서 해당 Step이 실패했을 때 재시도 전략도 구성할 수 있다.
- 스케쥴러랑 배치랑은 아무관련이 없다.