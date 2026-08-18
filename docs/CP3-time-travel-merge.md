# Checkpoint 3: Time Travel, ACID MERGE và Rollback

Notebook tương ứng: `notebooks/03_time_travel.py`

## 1. Mục tiêu của checkpoint

Checkpoint này xây dựng một lịch sử phiên bản có chủ đích cho một bảng, rồi dùng chính lịch sử đó để chứng minh ba khả năng nền tảng của Delta Lake: đọc dữ liệu ở một phiên bản cũ (time travel), gộp dữ liệu mới vào bảng theo kiểu vừa cập nhật vừa chèn trong một giao dịch nguyên tố (ACID MERGE, còn gọi là upsert), và khôi phục bảng về trạng thái sạch sau khi phát hiện dữ liệu lỗi (RESTORE).

## 2. Điều kiện tiên quyết

Đã hoàn thành Checkpoint 0, 1 và 2. Notebook tự sinh dữ liệu, không phụ thuộc `make data`.

## 3. Các bước triển khai

### Bước 1. Tạo v0, bản nạp ban đầu 100.000 dòng

```python
v0 = pl.DataFrame({
    "customer_id": list(range(100_000)),
    "status":      ["active"] * 100_000,
    "score":       [i % 1000 for i in range(100_000)],
})
write_deltalake(table_path, v0.to_arrow(), mode="overwrite")
```

### Bước 2. Tạo v1, thêm cột tier bằng cách ghi đè có tiến hoá schema

```python
v1 = (pl.from_arrow(DeltaTable(table_path).to_pyarrow_table())
        .with_columns(pl.when(pl.col("score") > 800).then(pl.lit("gold"))
                        .otherwise(pl.lit("silver")).alias("tier")))
write_deltalake(table_path, v1.to_arrow(), mode="overwrite", schema_mode="overwrite")
```

Khác với Checkpoint 1 nơi cột mới được thêm bằng `mode="append"` cộng `schema_mode="merge"`, ở đây notebook đọc lại toàn bộ bảng, tính thêm cột `tier` bằng logic điều kiện trên `score`, rồi ghi đè toàn bộ bằng `mode="overwrite"` cộng `schema_mode="overwrite"`. Đây là cách áp dụng khi bạn cần biến đổi dữ liệu đã có sẵn để tạo ra cột mới, không đơn giản là thêm cột trống cho dòng mới.

### Bước 3. Tạo v2, thực thi ACID MERGE upsert 100.000 dòng

```python
updates = pl.DataFrame({
    "customer_id": list(range(50_000, 150_000)),
    "status": ["vip"] * 100_000, "score": [999] * 100_000, "tier": ["platinum"] * 100_000,
})
(DeltaTable(table_path)
    .merge(source=updates.to_arrow(),
           predicate="t.customer_id = s.customer_id",
           source_alias="s", target_alias="t")
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute())
```

Đây là điểm trung tâm của checkpoint. `updates` chứa `customer_id` từ 50.000 đến 149.999, tức là chồng lấp một nửa với bảng gốc (0 đến 99.999). Điều kiện `predicate` so khớp theo `customer_id`. `when_matched_update_all()` nghĩa là với 50.000 khách hàng đã tồn tại (50.000 đến 99.999), toàn bộ cột của họ được cập nhật thành giá trị mới (status="vip", tier="platinum"). `when_not_matched_insert_all()` nghĩa là với 50.000 khách hàng chưa tồn tại (100.000 đến 149.999), một dòng mới được chèn thêm. Toàn bộ 100.000 thay đổi này xảy ra trong một giao dịch nguyên tố duy nhất, tương đương một mệnh lệnh `MERGE INTO` trong Spark SQL.

### Bước 4. Tạo v3, mô phỏng một lần ghi dữ liệu lỗi

```python
bad = pl.DataFrame({
    "customer_id": list(range(50)), "status": [None] * 50,
    "score": [-1] * 50, "tier": ["UNKNOWN"] * 50,
})
write_deltalake(table_path, bad.to_arrow(), mode="append")
```

Đây mô phỏng một sự cố thường gặp trong thực tế: một job upstream có lỗi (bug) đã đẩy 50 dòng có `score=-1`, một giá trị không hợp lệ về nghiệp vụ, vào bảng.

### Bước 5. Kiểm tra lịch sử và truy vấn theo phiên bản cũ

```python
for h in DeltaTable(table_path).history():
    print(f"v{h['version']}  {h['operation']}")

v0_count = DeltaTable(table_path, version=0).to_pyarrow_table().num_rows
v1_cols  = DeltaTable(table_path, version=1).schema().to_arrow().names
```

Truyền tham số `version=0` khi khởi tạo `DeltaTable` sẽ trả về đúng trạng thái bảng tại thời điểm cam kết phiên bản 0, trước khi có cột `tier`, trước khi có MERGE, trước khi có dữ liệu lỗi. Đây chính là time travel, tương đương `option("versionAsOf", 0)` trong Spark.

### Bước 6. Khôi phục bảng về trạng thái sạch bằng RESTORE

```python
dt = DeltaTable(table_path)
dt.restore(2)
```

`restore(2)` không xoá lịch sử, nó tạo ra một phiên bản mới (v4) mà nội dung của nó bằng đúng nội dung tại v2, thời điểm ngay sau MERGE và trước khi có dữ liệu lỗi. Vì restore chính nó cũng là một giao dịch được ghi vào log, toàn bộ hành động này vẫn có thể kiểm chứng lại được sau này (fully auditable), khác với việc âm thầm sửa tay file trên đĩa.

### Bước 7. Xác nhận dữ liệu lỗi đã biến mất

```python
dt_after = DeltaTable(table_path)
bad_count = dt_after.to_pyarrow_table(filters=[("score", "<", 0)]).num_rows
```

Notebook cố tình đi qua delta-rs để lọc thay vì dùng DuckDB `delta_scan()` ở bước này, vì extension delta của DuckDB (tính đến bản 1.5.x) khắt khe hơn với các mục protocol được ghi ra sau một lần RESTORE, dễ gây ra lỗi `InvalidProtocolError` mang tính chất chủng tộc (race), nên đi thẳng qua delta-rs tránh được vấn đề này.

## 4. Kết quả mong đợi

`history()` cuối cùng phải có ít nhất 5 phiên bản: v0 (nạp ban đầu), v1 (thêm cột tier), v2 (MERGE), v3 (dữ liệu lỗi), và v4 (RESTORE). Số dòng có `score < 0` sau khi restore phải bằng 0. Notebook in ra `NB3 complete.` nếu cả bốn điều kiện đạt: lịch sử có ít nhất 5 phiên bản, lịch sử có chứa một dòng operation RESTORE, lịch sử có chứa một dòng operation MERGE, và số dòng lỗi bằng 0 sau khi restore.

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 12 điểm: 4 điểm cho việc `history()` hiển thị ít nhất 5 phiên bản bao gồm cả dòng RESTORE, 4 điểm cho việc MERGE upsert 100.000 dòng thành công, và 4 điểm cho việc RESTORE khôi phục đúng và số dòng `score < 0` về 0.

## 6. Sự cố thường gặp

Nếu bạn thấy thời gian MERGE hoặc RESTORE lâu bất thường, hãy kiểm tra máy có đang chạy song song nhiều notebook khác không, vì đường lightweight của lab dùng chung tài nguyên CPU/đĩa trên một máy. README ghi nhận thời gian mong đợi cho MERGE là dưới 60 giây (thực tế thường dưới 1 giây trên đường lightweight) và RESTORE dưới 30 giây.

## 7. Ý nghĩa kỹ thuật

Bài học cốt lõi: RESTORE không phải là "xoá dữ liệu xấu", nó là "thêm một giao dịch mới nói rằng trạng thái hiện tại phải giống trạng thái ở một phiên bản trước". Vì nó là một giao dịch, mọi bảo đảm ACID vẫn áp dụng, và bạn không bao giờ mất khả năng truy vết lại chuyện gì đã xảy ra, kể cả việc bạn đã từng rollback. Đây là khác biệt căn bản so với việc chạy `DELETE FROM table WHERE ...` trực tiếp trên một hệ quản trị dữ liệu không có transaction log.
