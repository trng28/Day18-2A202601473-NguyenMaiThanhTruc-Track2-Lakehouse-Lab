# Checkpoint 5: Apache Iceberg và Catalog như một Control Plane

Notebook tương ứng: `notebooks/05_iceberg_catalog.py`

## 1. Mục tiêu của checkpoint

Từ checkpoint này, lab chuyển sang phần "lakehouse 2026". Checkpoint 5 giới thiệu định dạng bảng mở thứ hai, Apache Iceberg, thông qua `pyiceberg` cùng một catalog SQLite cục bộ, không cần JVM, không cần server, không cần cloud. Nội dung quan trọng nhất không phải là Iceberg tự nó, mà là một sự dịch chuyển kiến trúc: catalog không còn chỉ là một bảng tra cứu tên sang đường dẫn, nó trở thành bộ lập kế hoạch truy vấn (query planner) và ranh giới an ninh (security boundary) của cả hệ thống. Iceberg 1.11 đưa việc lập kế hoạch quét dữ liệu (scan planning) lên phía server, còn Delta 4.1 giới thiệu catalog-managed tables, hai phe đối lập cùng đi đến một kết luận giống nhau.

## 2. Điều kiện tiên quyết

Đã hoàn thành các checkpoint trước. Notebook tự tạo catalog và bảng của riêng nó, dùng tên catalog `nb5`, tách biệt hoàn toàn với catalog của Checkpoint 6 và 8 (đặt tên `nb6`, `nb8`) để việc mở nhiều notebook cùng lúc trong Jupyter không làm hỏng dữ liệu của nhau.

## 3. Các bước triển khai

### Bước 1. Tạo catalog và namespace

```python
CAT = "nb5"
reset_catalog(CAT)
cat = catalog(CAT)
ns = namespace(cat, "lake")
```

`catalog()` trả về một `SqlCatalog` của `pyiceberg`, lưu metadata trong một file SQLite cục bộ. Đây là cùng một API mà bạn sẽ dùng để trỏ vào một catalog REST thật ở production như Polaris, Unity Catalog, Lakekeeper hay Glue, chỉ khác tham số cấu hình, code gọi API giữ nguyên.

### Bước 2. Tạo bảng thông qua catalog, không tự chọn đường dẫn

```python
SCHEMA = pa.schema([
    pa.field("event_id", pa.int64(), nullable=False),
    pa.field("ts", pa.timestamp("us"), nullable=False),
    pa.field("model", pa.string()),
    pa.field("latency_ms", pa.int64()),
    pa.field("cost_usd", pa.float64()),
])
tbl = cat.create_table(f"{ns}.llm_events", schema=SCHEMA)
```

Điểm cần chú ý: bạn không truyền vào một đường dẫn lưu trữ, catalog tự quyết định vị trí lưu (`tbl.location()`). Sự tách rời này chính là điều cho phép catalog sau này cấp quyền truy cập tạm thời (vend credentials), áp đặt bộ lọc theo dòng (row filter), và lập kế hoạch quét hộ bạn. Tham số `nullable=False` không mang tính hình thức, Iceberg theo dõi field nào là bắt buộc (required) và field nào là tuỳ chọn (optional) ở cấp metadata, một field bắt buộc không thể bị âm thầm bỏ qua khi ghi dữ liệu.

### Bước 3. Thêm hidden partitioning, tính năng thay thế mô hình của Hive

```python
from pyiceberg.transforms import DayTransform
with tbl.update_spec() as spec:
    spec.add_field("ts", DayTransform(), "ts_day")
```

Trong Hive, bạn phải phân vùng theo một cột dẫn xuất (ví dụ `dt=2026-08-05`) mà người dùng phải tự biết và tự lọc bằng tay, quên lọc là full scan cả bảng. Iceberg lưu chính bản thân phép biến đổi (`day(ts)`) trong metadata. Người dùng chỉ cần lọc trên cột thật là `ts`, engine tự suy ra phân vùng, cột `ts_day` không phải là cột bạn chèn dữ liệu vào, nó được dẫn xuất hoàn toàn tự động.

### Bước 4. Append 10 batch ngày, mỗi lần append là một snapshot

```python
for day in range(1, N_DAYS + 1):
    tbl.append(day_batch(day))
```

Mỗi lệnh `append()` là một giao dịch nguyên tố, tạo ra một snapshot mới, cùng bảo đảm ACID mà Delta cung cấp, chỉ biểu diễn bằng một cây metadata khác.

### Bước 5. Đo tỉ lệ pruning khi lọc trên cột thật ts

```python
scan_all = tbl.scan()
scan_one_day = tbl.scan(row_filter="ts >= '2026-08-05T00:00:00' and ts < '2026-08-06T00:00:00'")
files_all = len(list(scan_all.plan_files()))
files_one = len(list(scan_one_day.plan_files()))
PRUNE_RATIO = files_all / max(files_one, 1)
assert PRUNE_RATIO >= 5
```

`plan_files()` chính là bước lập kế hoạch: cho một bộ lọc, những file dữ liệu nào thực sự cần đọc. Từ Iceberg 1.11, bước này có thể chạy hẳn bên trong catalog server, khiến client trên laptop không cần tải cả cây manifest về máy. Chú ý: bộ lọc dùng cột `ts`, không dùng `ts_day`, đây chính là điểm khác biệt so với Hive được nói ở bước 3.

Notebook còn quy đổi số liệu này ra chi phí thật: nếu mỗi file nặng 512 MB và giá quét dữ liệu là 5 đô la mỗi TB (giá niêm yết phổ biến của Athena hay BigQuery), một người dùng kiểu Hive quên viết điều kiện phân vùng sẽ trả một khoản tiền lãng phí đáng kể mỗi ngày ở quy mô 10.000 truy vấn mỗi ngày. Đây là cách biến một khái niệm trừu tượng ("hidden partitioning tốt hơn") thành một con số đô la cụ thể.

### Bước 6. Đi qua cây metadata ba tầng

```python
snaps = tbl.inspect.snapshots()
mans = tbl.inspect.manifests()
files = tbl.inspect.files()
```

Cấu trúc cây là: catalog trỏ tới `metadata.json` (chứa schema, các partition spec, con trỏ snapshot hiện tại), `metadata.json` trỏ tới một manifest list (một danh sách cho mỗi snapshot), manifest list trỏ tới các manifest file (mỗi manifest chứa thống kê min/max và số lượng dòng của các file dữ liệu mà nó theo dõi), và cuối cùng là các file dữ liệu Parquet thật. Mỗi tầng tồn tại để tầng phía trên có thể bỏ qua công việc ở tầng phía dưới. Notebook còn tính tỉ lệ dung lượng metadata so với dữ liệu (`meta_bytes / data_bytes`) để cho thấy: ở quy mô 10 dòng mỗi file, tỉ lệ này trông có vẻ vô lý, nhưng ở quy mô 512 MB mỗi file trong thực tế, tỉ lệ chỉ còn khoảng 0.1%. File nhỏ làm bạn trả giá hai lần, vừa nhiều file dữ liệu, vừa nhiều metadata phải lập kế hoạch qua.

### Bước 7. Tiến hoá schema theo field ID, không theo tên hay vị trí

```python
with tbl.update_schema() as upd:
    upd.rename_column("latency_ms", "latency_millis")
```

Parquet định vị cột theo vị trí, Hive định vị theo tên, cả hai đều gãy khi bạn đổi tên hoặc đổi thứ tự cột. Iceberg gán cho mỗi field một số nguyên cố định vĩnh viễn (`field_id`), tên chỉ là một nhãn gắn lên số đó. Vì vậy, đổi tên cột là một thao tác chỉ sửa metadata, không viết lại một byte dữ liệu nào. Notebook chứng minh điều này bằng cách in `field_id` trước và sau khi đổi tên, `latency_ms` đổi thành `latency_millis` nhưng vẫn giữ `field_id=4`.

### Bước 8. Tiến hoá partition spec, điều Hive không thể làm

```python
from pyiceberg.transforms import IdentityTransform
with tbl.update_spec() as spec:
    spec.add_field("model", IdentityTransform(), "model_id")
tbl.append(day_batch(11)...)
specs_in_use = set(tbl.inspect.files().column("spec_id").to_pylist())
```

Khi lưu lượng dữ liệu tăng, phân vùng theo ngày có thể trở nên quá thô. Trong Hive, thay đổi này đòi hỏi viết lại toàn bộ bảng theo cấu trúc thư mục mới. Trong Iceberg, bạn chỉ thay đổi spec, dữ liệu cũ giữ nguyên chỗ cũ, mỗi file dữ liệu tự nhớ nó được viết bởi spec nào (`spec_id`). Sau bước này, bảng có ít nhất 2 spec khác nhau cùng tồn tại và toàn bộ bảng vẫn đọc được bình thường qua một câu `scan()` duy nhất.

## 4. Kết quả mong đợi

Notebook in ra tỉ lệ pruning khi lọc trên `ts` (mục tiêu ít nhất 5 lần), số snapshot đã tích lũy (ít nhất 10, tương ứng 10 lần append cộng 1 lần append cuối ở bước 8), xác nhận `field_id=4` không đổi sau khi đổi tên cột, và số lượng `spec_id` phân biệt (ít nhất 2). Kết thúc bằng `NB5 complete.`

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 13 điểm: 3 điểm cho việc tạo bảng qua catalog với partition spec dùng `day(ts)`, 5 điểm cho việc đo được tỉ lệ pruning từ 5 lần trở lên bằng `plan_files()` khi lọc trên `ts`, 1 điểm cho việc đi qua đủ cây metadata ba tầng và báo cáo tỉ lệ dung lượng metadata trên dữ liệu, và 4 điểm cho việc đổi tên cột giữ nguyên field_id cộng ít nhất 2 partition spec cùng tồn tại mà bảng vẫn đọc được.

## 6. Sự cố thường gặp

Nếu bạn mở đồng thời Checkpoint 5, 6 và 8 hoặc chạy `make smoke` trong lúc một trong các notebook này đang thực thi, điều này an toàn vì mỗi notebook dùng tên catalog riêng (`nb5`, `nb6`, `nb8`, `smoke`), việc reset catalog ở đầu một notebook không đụng tới catalog của notebook khác.

## 7. Ý nghĩa kỹ thuật

Bài học lớn nhất của checkpoint này nằm ở khoảnh khắc bước 5 và bước 7: hidden partitioning loại bỏ hẳn khả năng người dùng quên viết điều kiện phân vùng (vì không có cột phân vùng nào để quên), và field ID cố định loại bỏ hẳn khả năng một phép đổi tên cột làm hỏng dữ liệu lịch sử. Cả hai đều là ví dụ về việc kiến trúc metadata tốt loại bỏ luôn cơ hội gây lỗi, thay vì chỉ dựa vào quy trình vận hành cẩn thận của con người.
