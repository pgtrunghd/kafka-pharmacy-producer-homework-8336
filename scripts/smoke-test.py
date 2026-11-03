#!/usr/bin/env python3
"""Kiểm tra API thật và đọc lại JSON, message key, partition từ Kafka."""
import json
import os
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

base = os.environ.get("API_URL", "http://localhost:8082")
evidence = Path(__file__).resolve().parent.parent / "evidence"
evidence.mkdir(exist_ok=True)
responses = []
for medicine, quantity in [("MED-001", 2), ("MED-001", 3), ("MED-002", 1)]:
    request = urllib.request.Request(base + "/api/sales", data=json.dumps(
        {"medicineId": medicine, "quantity": quantity}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=25) as response:
        assert response.status == 201
        responses.append(json.load(response))
assert responses[0]["partition"] == responses[1]["partition"], "Cùng medicineId phải cùng partition"
try:
    urllib.request.urlopen(urllib.request.Request(base + "/api/sales",
        data=b'{"medicineId":"","quantity":0}',
        headers={"Content-Type": "application/json"}), timeout=10)
    raise AssertionError("Dữ liệu sai phải bị từ chối")
except urllib.error.HTTPError as error:
    assert error.code == 400
command = ["docker", "compose", "exec", "-T", "kafka",
    "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9094",
    "--topic", "medicine-stock-events", "--from-beginning", "--timeout-ms", "10000",
    "--property", "print.key=true", "--property", "print.partition=true"]
result = subprocess.run(command, capture_output=True, text=True, timeout=35,
    cwd=Path(__file__).resolve().parent.parent)
if result.returncode not in (0, 1):
    raise RuntimeError(result.stderr)
records = {}
for line in result.stdout.splitlines():
    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue
    partition, key, value = parts
    event = json.loads(value)
    records[event["orderId"]] = (int(partition.split(":")[-1]), key, event)
for response in responses:
    partition, key, event = records[response["event"]["orderId"]]
    assert event == response["event"]
    assert key == event["medicineId"]
    assert partition == response["partition"]
    assert set(event) == {"orderId", "medicineId", "quantity", "timestamp"}
(evidence / "api-responses.json").write_text(json.dumps(responses, ensure_ascii=False, indent=2))
(evidence / "kafka-records.txt").write_text(result.stdout)
print("PASS: 3 HTTP 201, JSON và medicineId key đúng, cùng key cùng partition, dữ liệu sai HTTP 400")
