# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lê Hoàng Đạt / 2A202602583
**Repo:** `K4-Track02-Day17-Data-Pipeline-Engineering`
**Commit bài nộp:** N/A (chưa commit/push)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Không dùng
**Nguồn tham khảo khác (nếu có):** N/A

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 ticket; `T-91` xuất hiện 3 lần với `low/open` và `high/open`; `silver_tickets` không bằng state mới nhất. | `gold_feature_daily` không khớp full recompute; `u05` ở 2026-08-12 chỉ đạt `(2,1,0)` thay vì `(5,3,1)`; `LOOKBACK_DAYS = 0` nhưng P99 là 3 ngày. | `T-97` vẫn còn ở Silver, snapshot training và RAG; `is_deleted` còn `False`; detector fail ở 3 check xoá. |
| **Nguyên nhân gốc** | Code dùng `INSERT` vào `silver_tickets` thay vì `MERGE` theo `ticket_id` và không lọc trạng thái mới nhất bằng `_lsn` / CDC ordering. | `config.LOOKBACK_DAYS` bị đặt `0`, nên tính toán `gold_feature_daily` xem xét không đủ dải window cho event đến muộn. | `ticket_id` được đọc chỉ từ `after`, nhưng delete CDC có `after = null`; ticket_id thực tế nằm ở Kafka `key`, và không có tombstone/soft-delete handling. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE INTO silver_tickets` trên `ticket_id`, cập nhật chỉ khi `s._lsn > t._lsn`; `pipeline/staging.py`: đọc `ticket_id` từ `key/before/after` và ghép latest state. | `pipeline/config.py`: đổi `LOOKBACK_DAYS` từ `0` sang `3`; `gold_feature_daily` dùng window `[day - LOOKBACK_DAYS, day]` để tính event-date đúng. | `pipeline/staging.py`: fallback `ticket_id` + `before`/`after` và `silver.py`: tombstone row với `is_deleted = true`, các field PII còn lại `NULL`. |
| **Khái niệm trên slide** | Silver — có khoá, cập nhật theo order của CDC, dedup không chịu overwrite từ batch cũ. | Gold — đúng time window, event time khác ingest time; lookback phải nắm tiếng trễ của Bronze. | CDC log-based — delete có `after = null`, tombstone phải được giữ như một sự kiện trạng thái, không chỉ bỏ qua. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: vì cùng một thực thể phải có một hàng duy nhất trong Silver, còn Gold cần recompute theo event-date với window rõ ràng và idempotent khi rerun.
- Tombstone thay vì xoá hẳn hàng trong Silver: vì CDC delete dù là `after = null` vẫn cần giữ LSN và is_deleted để loại ticket khỏi snapshot mới nhất và RAG mà không mất lịch sử cập nhật của ticket.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: vì snapshot phải bất biến để các phiên bản training và checksum ổn định khi rerun một ngày cũ hay chạy backfill lại.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: vì dữ liệu phân tích ở đây nhỏ, local và cần khớp correctness hơn là scale tuyệt đối; DuckDB đủ nhanh và dbt dễ đối chiếu checksum.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - Cách tiếp cận hợp lý là: snapshot cũ được giữ như một bức ảnh thời điểm bất biến để reproduce và audit, nhưng snapshot mới nhất / live training set phải loại đối tượng đã xoá bằng tombstone và filter `WHERE l._op <> 'd'`; nếu cần tuân thủ quyền xoá thật sự, phải có chính sách retention riêng cho dữ liệu cũ, không phá vỡ tính immutable của snapshot lịch sử.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - Giới hạn của regex là chỉ phát hiện dạng mẫu; tên người cần dùng NER / classification / allowlist ở tầng Silver hoặc trước khi ra Gold, với metric: kiểm tra số bản ghi còn chứa tên người, tỉ lệ PII trên phạm vi dữ liệu, và unit tests cho các trường có chứa tên/email/số điện thoại.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS

$ .\.venv\Scripts\python.exe -m pytest -q
..................................                                       [100%]

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\dbt.exe build --event-time-start 2026-08-10 --event-time-end 2026-08-17
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 6.63 seconds.
Completed successfully
PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
