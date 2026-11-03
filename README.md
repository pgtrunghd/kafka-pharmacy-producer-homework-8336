# Bài 2 — Producer gửi sự kiện Đơn hàng thuốc

Đề bài: https://portal.rikkei.edu.vn/learn/159/homework/8336

Ứng dụng `pharmacy-service` dùng Spring Boot 3.5.6, Java 21 và Spring Kafka. Khi thanh toán, `KafkaTemplate<String, OrderEvent>` gửi JSON vào topic `medicine-stock-events`, sử dụng `medicineId` làm message key.

## Chạy ứng dụng

Yêu cầu: JDK 21, Docker Desktop, Python 3 để chạy smoke test. Cổng 8082 và 9094 còn trống. Gradle Wrapper đã có trong repo.

```bash
docker compose up -d --wait --wait-timeout 180
./gradlew test bootJar
java -jar build/libs/pharmacy-service-1.0.0.jar
```

Mở http://localhost:8082, nhập mã thuốc và số lượng rồi bấm **Thanh toán**. Hoặc gọi API:

```bash
curl -i http://localhost:8082/api/sales \
  -H 'Content-Type: application/json' \
  -d '{"medicineId":"MED-001","quantity":2}'
```

Cấu hình mặc định Kafka là `localhost:9094` để chạy song song với bài 1 ở cổng 9092. Có thể đổi bằng biến `KAFKA_BOOTSTRAP_SERVERS`; cổng HTTP đổi bằng `SERVER_PORT`.

## Luồng xử lý

1. `POST /api/sales` kiểm tra mã thuốc không rỗng, số lượng nguyên dương.
2. Server tạo `orderId` UUID và `timestamp` UTC để đóng gói `OrderEvent`.
3. `OrderProducer` gọi `template.send(topic, event.medicineId(), event)`.
4. `JsonSerializer` chuyển đối tượng thành JSON. Producer cấu hình `acks=all` và idempotence.
5. API chờ Kafka xác nhận rồi trả HTTP 201, thông báo thành công, event và metadata topic/partition/offset. Khi chưa xác nhận được, API trả HTTP 503; dữ liệu sai trả HTTP 400.

Ví dụ dữ liệu event:

```json
{
  "orderId": "UUID-do-server-tao",
  "medicineId": "MED-001",
  "quantity": 2,
  "timestamp": "2026-11-03T14:00:00Z"
}
```

Topic được khai báo bằng bean `NewTopic`: 3 partitions, replication factor 1. Không chỉ định partition khi gửi; Kafka chọn theo key. Các event cùng medicineId vào cùng partition khi số partition giữ nguyên. Không tăng số partition nếu muốn giữ nguyên ánh xạ hiện tại.

## Kiểm chứng

```bash
./gradlew test
# Giữ ứng dụng chạy trong terminal khác:
python3 scripts/smoke-test.py
```

- Kiểm thử MVC: thành công sau broker acknowledgement, đầu vào sai không gửi event, gửi thất bại trả HTTP 503.
- Smoke test gọi API 3 lần, đọc lại tin nhắn từ Kafka thật, kiểm tra đủ 4 trường JSON, key đúng medicineId, partition trong phản hồi đúng thực tế và hai event cùng thuốc cùng partition. Kiểm tra thêm HTTP 400.
- `evidence/api-responses.json` và `evidence/kafka-records.txt` là kết quả chạy thực tế.

Đọc tin nhắn thủ công:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 --topic medicine-stock-events \
  --from-beginning --property print.key=true --property print.partition=true
```

## Cấu trúc

- `OrderEvent`: dữ liệu sự kiện theo đề bài.
- `OrderProducer`: gửi sự kiện bằng KafkaTemplate, dùng medicineId làm key.
- `SaleController`: API thanh toán và phản hồi sau acknowledgement.
- `KafkaTopicConfig`: khởi tạo topic 3 partitions.
- `application.yml`: cấu hình JSON serializer, producer và cổng.
- `src/main/resources/static/index.html`: form thanh toán gọi API.

## Phạm vi thực hành

API mô phỏng thanh toán và phát sự kiện; chưa xử lý tiền thật, cơ sở dữ liệu hoặc trừ kho (phần Consumer thuộc bài tiếp theo). Idempotence của producer không chống gửi trùng khi người dùng gọi API nhiều lần. Nếu timeout, trạng thái gửi có thể chưa xác định, không nên tự động gọi lại trong hệ thống thực tế khi chưa có cơ chế chống trùng nghiệp vụ.

`docker compose stop` giữ dữ liệu trong container. `docker compose down` xóa container và dữ liệu; topic được tạo lại khi service khởi động.

## Tài liệu tham khảo

- https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html
- https://kafka.apache.org/41/getting-started/docker/
