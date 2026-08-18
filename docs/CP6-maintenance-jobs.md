# Checkpoint 6: Năm công việc bảo trì bảng bắt buộc

Notebook tương ứng: `notebooks/06_maintenance.py`

## 1. Mục tiêu của checkpoint

Đây là checkpoint nặng nhất về nội dung vận hành trong toàn bộ lab. Slide gọi bài toán small files là nguyên nhân gây sự cố production phổ biến nhất của một lakehouse, phổ biến hơn tổng tất cả các nguyên nhân khác cộng lại, và nguyên nhân của nó không phải là code sai, mà là hành vi ingest bình thường cộng với việc thiếu một cron job bảo trì. Checkpoint này bắt bạn tự chạy và tự đo cả bốn công việc bảo trì bắt buộc, cộng thêm một công việc thứ năm, trên cả hai định dạng Delta và Iceberg.

## 2. Điều kiện tiên quyết

Đã hoàn thành các checkpoint trước, đặc biệt là Checkpoint 2 (khái niệm compact và Z-order) và Checkpoint 5 (khái niệm snapshot và catalog Iceberg).

## 3. Các bước triển khai

### Bước 0. Tạo lại bài toán: 200 micro-batch

```python
for b in range(N_BATCHES):
    batch = pa.table({...})
    write_deltalake(TABLE, batch, mode="append" if b else "overwrite")
```

Giống Checkpoint 2, nhưng lần này notebook định nghĩa một hàm `snapshot_metrics()` đo bốn số quyết định: số file dữ liệu, số byte dữ liệu, số byte log, và số dòng, để dùng làm đường mốc (baseline) so sánh trước và sau mỗi công việc bảo trì.

Notebook còn quy đổi ngay số file baseline ra chi phí request tới object storage: mỗi file được quét tốn một lệnh GET, ở mức 50.000 truy vấn full-scan mỗi ngày, 200 file so với 4 file compact tạo ra chênh lệch chi phí GET đáng kể mỗi ngày, minh chứng rằng object storage tính tiền theo số lượng request, không chỉ theo số byte.

### Bước 1. Job 1, Compaction

```python
metrics = dt.optimize.compact(target_size=TARGET_SIZE)
```

Nén nhiều file nhỏ thành ít file lớn hơn. Đây là một commit mới, các file cũ không bị xoá ngay, chúng bị đánh dấu bỏ (tombstone), việc này quan trọng cho Job 3 phía sau. Notebook cũng chỉ ra một điều dễ bị hiểu lầm: ngay sau compact, tổng số byte dữ liệu trên đĩa tăng lên tạm thời, vì file mới được viết trước khi file cũ được thu hồi, tức bạn phải trả tiền lưu trữ gấp đôi trong một khoảng thời gian ngắn, cần dự trù chi phí này.

### Bước 2. Job 2, Clustering, đo bằng chất lượng thống kê, không đo bằng đồng hồ

```python
def files_touched(target=TARGET_USER):
    aa = pa.table(DeltaTable(TABLE).get_add_actions(flatten=True)).to_pylist()
    return sum(1 for f in aa if f["min.user_id"] <= target <= f["max.user_id"])

before_cluster = files_touched()
DeltaTable(TABLE).optimize.z_order(["user_id"], target_size=TARGET_SIZE)
after_cluster = files_touched()
```

Checkpoint 2 đo Z-order bằng đồng hồ thực, ở đây notebook đo bằng chỉ số xác định: có bao nhiêu file mà khoảng `[min, max]` của `user_id` có khả năng chứa giá trị mục tiêu 12.345. Trước khi cluster, dữ liệu chưa được sắp xếp nên các khoảng min/max chồng lấp lên nhau nhiều, hầu như file nào cũng phải mở. Sau Z-order, số file cần mở giảm mạnh, tỉ lệ file được bỏ qua (`1 - after_cluster/total_files`) phải đạt tối thiểu 50%.

### Bước 3. Job 3, hết hạn snapshot hoặc phiên bản

```python
doomed = dt.vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False)
dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
```

Mọi commit từ đầu tới giờ vẫn đọc được qua time travel, và điều đó không miễn phí, các file đã tombstone vẫn nằm trên đĩa và vẫn bị tính tiền lưu trữ. `retention_hours=0` chỉ dùng trong lab để việc thu hồi hiện ra ngay lập tức, production luôn dùng ít nhất 168 giờ (7 ngày), vì đặt về 0 sẽ phá khả năng time travel và có thể làm hỏng một reader đang đọc giữa chừng.

Phần Iceberg của bước này quan trọng hơn phần Delta, vì nó chứa một trong hai "phát hiện đo được" nổi bật nhất của cả lab:

```python
doomed_ids = [s.snapshot_id for s in ice.snapshots()[:-KEEP_LAST]]
ice.maintenance.expire_snapshots().by_ids(doomed_ids).commit()
```

Sau lệnh này, số snapshot giảm từ 20 xuống 3, nhưng số file `.avro` (manifest list) trên đĩa không đổi một byte nào, và dung lượng metadata thực ra còn tăng lên (vì expiry viết ra một `metadata.json` mới). Đây không phải là lỗi, nhiệm vụ của expiry chỉ là làm cho các file trở nên "không còn được tham chiếu" (unreferenced), việc xoá file thật là một công việc khác, chính là Job 4.

### Bước 4. Job 4, dọn file rác không ai nhìn thấy (orphan)

```python
for i in range(3):
    orphan = Path(TABLE) / f"part-9999{i}-crashed-writer-c000.snappy.parquet"
    pq.write_table(pa.table({...}), orphan)
    os.utime(orphan, (old, old))   # 30 ngày tuổi
```

Notebook tự tạo ra ba file mô phỏng ba writer bị crash trước khi commit, các file này nằm trên đĩa nhưng không có mục nào trong transaction log tham chiếu tới chúng, nghĩa là chúng vô hình với `history()`, với `file_uris()`, và với mọi dashboard đo dựa trên metadata.

```python
still = DeltaTable(TABLE).vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False)
```

Đây là phát hiện đo được thứ hai và cũng là phát hiện quan trọng nhất của cả lab: chạy lại `vacuum()` trên các orphan đã 30 ngày tuổi, `deltalake` (bản Rust/Python dùng trong lab) vẫn không tìm thấy chúng. Lý do là `vacuum()` chỉ thu hồi file đã bị tombstone trong log, một file chưa từng được commit thì chưa từng bị tombstone, log không hề biết nó tồn tại. Spark's VACUUM có thêm một bước quét trực tiếp thư mục bảng nên có thể bắt được trường hợp này, nhưng bạn không nên mặc định rằng engine của mình luôn làm bước quét đó, phải tự kiểm chứng hoặc tự chạy phép so sánh.

```python
def find_orphans(table_path, min_age_hours=24):
    referenced = {os.path.realpath(u.replace("file://", "")) for u in DeltaTable(table_path).file_uris()}
    cutoff = time.time() - min_age_hours * 3600
    orphans = []
    for f in Path(table_path).rglob("*.parquet"):
        if "_delta_log" in f.parts:
            continue
        if os.path.realpath(f) not in referenced and f.stat().st_mtime < cutoff:
            orphans.append(str(f))
    return orphans
```

Notebook bắt bạn tự viết đúng thuật toán mà `remove_orphan_files` thực hiện bên trong: phép hiệu tập hợp giữa "file có trên đĩa" và "file được metadata sống tham chiếu", cộng với một điều kiện tuổi tối thiểu (age guard). Điều kiện tuổi này không phải tuỳ chọn, thiếu nó bạn có thể xoá nhầm file mà một writer đang chạy đồng thời đã viết ra nhưng chưa commit xong, làm hỏng bảng.

Phần Iceberg tương ứng dùng phép hiệu tương tự trên manifest list:

```python
def find_iceberg_orphans(table):
    live = {Path(s.manifest_list).name for s in table.snapshots()}
    return [f for f in ice_meta.glob("snap-*.avro") if f.name not in live]
```

Notebook nhấn mạnh: Job 3 và Job 4 là một cặp không thể tách rời, chạy expiry mà không quét orphan chính là lý do các nhóm vận hành thường thắc mắc "đã expire snapshot rồi mà hoá đơn lưu trữ không giảm".

### Bước 5. Job 5, viết checkpoint log

```python
DeltaTable(TABLE).create_checkpoint()
```

200 commit tương đương 200 file JSON trong `_delta_log/`. Một reader khởi động lạnh (cold start) phải đọc lại toàn bộ 200 file đó để biết trạng thái hiện tại của bảng. `create_checkpoint()` gộp toàn bộ trạng thái đó vào một file Parquet duy nhất cộng với con trỏ `_last_checkpoint`, để lần đọc sau chỉ cần đọc một checkpoint cộng vài file JSON mới nhất. Với bảng CDC hoặc streaming, đây là khác biệt giữa thời gian khởi động 200 milli giây và 20 giây.

## 4. Kết quả mong đợi

Notebook in ra số liệu trước và sau cho cả năm công việc, và kết thúc bằng một khối kiểm tra chín điều kiện: compact giảm số file ít nhất 10 lần, clustering giúp bỏ qua ít nhất 50% số file cho một truy vấn điểm, vacuum thu hồi được byte thật trên Delta, ba orphan Delta được tìm thấy và xoá đúng, không còn orphan nào sót lại, checkpoint đã được viết ra cùng `_last_checkpoint`, Iceberg giảm đúng về 3 snapshot, toàn bộ manifest list mồ côi trên Iceberg đã được dọn, và dữ liệu Iceberg vẫn còn đủ 2.000 dòng sau tất cả các bước trên. Kết thúc bằng `NB6 complete.`

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 13 điểm, chia đều cho năm công việc: 4 điểm cho compaction giảm ít nhất 10 lần số file kèm số liệu trước/sau, 3 điểm cho clustering chứng minh được ít nhất 50% file có thể bỏ qua từ thống kê min/max, 3 điểm cho snapshot/version expiry (Delta vacuum thu hồi byte, Iceberg giảm về 3 snapshot), 2 điểm cho việc tìm và xoá đúng 3 orphan Delta đã cấy sẵn cộng việc dọn manifest list mồ côi trên Iceberg, và 1 điểm cho việc checkpoint log đã được viết.

## 6. Sự cố thường gặp

README nhấn mạnh hai phát hiện đo được của checkpoint này chính là hai điều mà nhiều người vẫn tin sai: VACUUM không dọn được orphan chưa từng commit, và expire_snapshots của Iceberg không xoá file nào cả, chỉ làm file trở nên không được tham chiếu. Nếu bài nộp của bạn chỉ in ra con số mà không nêu được đúng lý do phía sau hai phát hiện này, đây chính là điểm mà rubric phân biệt một bài nộp "đạt" với một bài nộp "tốt".

## 7. Ý nghĩa kỹ thuật

Bài học tổng quát nhất của checkpoint này: một lakehouse không tự dọn dẹp chính nó. Bốn công việc bảo trì này không phải các tính năng tuỳ chọn để tối ưu thêm, chúng là điều kiện cần để một bảng không phình ra vô hạn về chi phí và về thời gian lập kế hoạch truy vấn. Việc tự tay viết thuật toán tìm orphan bằng phép hiệu tập hợp, thay vì chỉ gọi một hàm có sẵn, giúp bạn hiểu chính xác giới hạn của công cụ mình đang dùng, một hiểu biết bạn sẽ cần khi công cụ đó thay đổi hành vi ở một bản phát hành sau.
