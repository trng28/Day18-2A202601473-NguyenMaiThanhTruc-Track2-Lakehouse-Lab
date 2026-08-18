# Checkpoint 1: Delta Lake Basics và Transaction Log

Notebook tương ứng: `notebooks/01_delta_basics.py`

## 1. Mục tiêu của checkpoint

Checkpoint này giới thiệu ba trụ cột của Delta Lake bằng thao tác thực tế thay vì chỉ đọc slide: transaction log (nhật ký giao dịch) là nguồn sự thật duy nhất của bảng, schema enforcement (ràng buộc schema) chặn dữ liệu sai kiểu tại thời điểm ghi, và schema evolution (tiến hoá schema) cho phép mở rộng bảng một cách có kiểm soát khi bạn chủ động cho phép. Stack sử dụng là `deltalake` (delta-rs) kết hợp Polars và DuckDB, không cần Spark, không cần JVM.

## 2. Điều kiện tiên quyết

Đã hoàn thành Checkpoint 0, virtual environment đã cài đặt đầy đủ. Notebook này không phụ thuộc vào `make data` hay `make data-ai`, nó tự tạo dữ liệu mẫu ngay trong notebook.

## 3. Các bước triển khai

### Bước 1. Khởi tạo đường dẫn bảng

```python
import _setup
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "users_delta")
reset(table_path)
```

`path("scratch", "users_delta")` trả về đường dẫn tuyệt đối `_lakehouse/scratch/users_delta`, tự tạo thư mục cha nếu chưa có. `reset()` xoá sạch bảng cũ nếu tồn tại, giúp việc chạy lại notebook nhiều lần luôn cho kết quả giống nhau (idempotent rerun).

### Bước 2. Ghi bảng Delta đầu tiên

```python
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "age": [30, 25, 35],
    "city": ["Hanoi", "HCMC", "Danang"],
})
write_deltalake(table_path, df.to_arrow(), mode="overwrite")
```

Đây là lần ghi đầu tiên, tương đương phiên bản (version) 0 của bảng. `write_deltalake` nhận một `pyarrow.Table` (chuyển đổi từ DataFrame Polars qua `.to_arrow()`), và tạo ra thư mục `_delta_log/` bên trong `table_path`.

### Bước 3. Đọc lại bảng và xem transaction log

```python
dt = DeltaTable(table_path)
print(pl.from_arrow(dt.to_pyarrow_table()))
for h in dt.history():
    print(f"v{h['version']}  {h['operation']}  {h.get('operationMetrics', {})}")
```

Mở file `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json` bằng một trình soạn thảo văn bản bất kỳ. Đây chính là nội dung mà `dt.history()` đọc và diễn giải lại. Đây cũng là cùng định dạng JSON mà Spark hay Databricks tạo ra, nên bạn có thể mở bảng này bằng Spark ở đường chạy khác của lab mà không cần chuyển đổi gì cả.

### Bước 4. Kiểm chứng schema enforcement

```python
bad = pl.DataFrame({"id": [4], "name": ["dan"], "age": ["thirty"], "city": ["Hue"]})
try:
    write_deltalake(table_path, bad.to_arrow(), mode="append")
    print("UNEXPECTED: bad write succeeded")
except Exception as e:
    print(f"BLOCKED by schema enforcement (expected): {type(e).__name__}")
```

Cột `age` trong bảng đang có kiểu số nguyên (Int64), nhưng ở đây ta cố tình ghi giá trị chuỗi `"thirty"`. Delta Lake kiểm tra schema của dữ liệu mới so với schema đã cam kết trong log trước khi cho phép ghi, và sẽ chặn thao tác này bằng một exception. Đây là điểm khác biệt căn bản so với việc ghi Parquet thô vào một thư mục, nơi không có ai kiểm tra schema hộ bạn.

### Bước 5. Tiến hoá schema có chủ đích

```python
new = pl.DataFrame({
    "id": [4], "name": ["dan"], "age": [28], "city": ["Hue"], "tier": ["premium"],
})
write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
```

Lần này dữ liệu mới thêm cột `tier` chưa từng tồn tại trong bảng. Nếu không có `schema_mode="merge"`, thao tác này sẽ bị chặn giống bước 4. Việc phải khai báo rõ `schema_mode="merge"` chính là ý nghĩa của "opt-in evolution": tiến hoá schema là một quyết định có chủ đích của người viết dữ liệu, không phải hành vi ngầm định.

### Bước 6. Truy vấn bằng DuckDB không qua mạng

```python
import duckdb
con = duckdb.connect()
con.register("users", DeltaTable(table_path).to_pyarrow_table())
tier_counts = con.sql("SELECT tier, count(*) AS n FROM users GROUP BY 1 ORDER BY 1").fetchall()
```

Lưu ý cách notebook đưa dữ liệu vào DuckDB: nó đăng ký trực tiếp một `pyarrow.Table` bằng `con.register()`, không gọi hàm `delta_scan()` của DuckDB. Lý do là `delta_scan()` tự động tải một extension DuckDB qua mạng lần đầu sử dụng, điều này hoạt động tốt ở nhà nhưng sẽ thất bại trong một phòng học bị chặn mạng. Đăng ký Arrow table là thao tác zero-copy và hoàn toàn offline.

## 4. Kết quả mong đợi

Ô cuối cùng của notebook chạy một khối kiểm tra và in ra bốn dòng trạng thái, tương ứng bốn tiêu chí: thư mục `_delta_log/` chứa ít nhất hai file JSON (một cho lần ghi đầu, một cho lần ghi merge schema), thao tác ghi sai schema đã bị chặn, cột `tier` đã được thêm vào schema sau khi dùng `schema_mode="merge"`, và truy vấn DuckDB trả về đúng hai nhóm tier. Nếu cả bốn đều đạt, notebook in ra dòng cuối `NB1 complete.`

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này đóng góp 8 điểm trong tổng 100 điểm của lab, chia thành ba phần: 4 điểm cho việc tạo bảng Delta và thấy được các commit JSON trong `_delta_log/`, 2 điểm cho việc schema enforcement chặn đúng thao tác ghi sai kiểu, và 2 điểm cho việc `schema_mode="merge"` thêm đúng cột `tier`.

## 6. Sự cố thường gặp

Nếu bạn thấy `AttributeError: 'DeltaTable' object has no attribute 'files'`, môi trường của bạn đang chạy `deltalake` bản 0.x, hãy chạy lại `make clean && make setup` để lấy bản 1.x. Nếu output của bước 5 hiển thị các dòng không theo thứ tự `id` cố định, đây là bình thường, Delta không đảm bảo giữ nguyên thứ tự ghi qua các lần append, notebook đã chủ động `sort("id")` trước khi in để kết quả ổn định.

## 7. Ý nghĩa kỹ thuật

Điểm mấu chốt cần nắm sau checkpoint này là: một bảng Delta không phải là một thư mục Parquet, mà là một thư mục Parquet cộng với một transaction log mô tả chính xác file nào thuộc về phiên bản nào của bảng. Chính transaction log này là thứ cho phép Checkpoint 3 làm được time travel và MERGE, và là thứ cho phép Checkpoint 6 phân biệt được file nào là rác (orphan) và file nào đang được bảng sử dụng.
