# Notification Service

스케줄 기반 푸시 알림 발송을 담당하는 서비스.

## 기술 스택

| 항목 | 내용 |
|------|------|
| Language | Scala (Akka HTTP) |
| Message Queue | Apache Kafka (Kafka Streams) |
| Push | Firebase FCM (Admin SDK) |
| 통신 | gRPC Client (User / Reservation / Content 서비스 호출) |
| 서비스 등록 | Netflix Eureka Client |

## 주요 기능

- 오늘 콘텐츠 스케줄 조회 (Content-Service gRPC 호출)
- 스케줄 시작 시간 기준 알림 예약 등록
- 스케줄 시작 시 대상 유저 조회 (User-Service gRPC 호출)
- Firebase FCM을 통한 푸시 알림 일괄 발송
- Kafka Streams를 통한 알림 이벤트 스트리밍 처리

### gRPC Client 연결
- **Content-Service** → 스케줄 ID / 시작 시간 조회
- **User-Service** → FCM 토큰 조회
- **Reservation-Service** → 예약 정보 조회
