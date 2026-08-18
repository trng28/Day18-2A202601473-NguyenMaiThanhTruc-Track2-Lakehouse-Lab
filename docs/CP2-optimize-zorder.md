# Checkpoint 2: Bài toán Small Files và OPTIMIZE cộng Z-ORDER

Notebook tương ứng: `notebooks/02_optimize_zorder.py`

## 1. Mục tiêu của checkpoint

Checkpoint này tái hiện một trong những lỗi vận hành phổ biến nhất của lakehouse: bài toán "small files" (quá nhiều file nhỏ), sau đó dùng hai công cụ để sửa nó là OPTIMIZE (nén file) và Z-ORDER (sắp xếp co cụm dữ liệu theo một cột). Ý tưởng cốt lõi cần nắm là giá trị thật của Z-order nằm ở khả năng bỏ qua file (file-skipping) dựa trên thống kê min/max của từng file, và khả năng này chỉ có ý nghĩa khi bảng có nhiều file. Nếu bạn nén hết mọi thứ về một file duy nhất, Z-order không còn gì để bỏ qua.

## 2. Điều kiện tiên quyết

Đã hoàn thành Checkpoint 0 và 1. Notebook tự tạo dữ liệu của riêng nó, không cần `make data`.

## 3. Các bước triển khai

### Bước 1. Chủ động tạo ra bài toán small files

```python
for batch in range(200):
    rows = pl.DataFrame({...})   # 5,000 dòng mỗi batch
    mode = "overwrite" if batch == 0 else "append"
    write_deltalake(table_path, rows.to_arrow(), mode=mode)
```

Notebook ghi 200 lần append liên tiếp, mỗi lần 5.000 dòng, mô phỏng đúng hành vi của một job ingest kiểu streaming với chu kỳ trigger ngắn (ví dụ Kafka đổ vào lakehouse mỗi 5 giây). Mỗi lần append tạo ra ít nhất một file Parquet mới, nên sau 200 lần, bảng có ít nhất 200 file nhỏ. Cột `payload` được thiết kế cố tình dài (khoảng 200 byte mỗi dòng) để dữ liệu đủ "nặng", tránh trường hợp compact gộp toàn bộ 1 triệu dòng vào một file 8 MB duy nhất, vì nếu vậy sẽ không còn gì để Z-order chứng minh khả năng bỏ file ở bước sau.

```python
files_before = len(dt.file_uris())
```

### Bước 2. Đo tốc độ truy vấn trước khi tối ưu

```python
def bench(label, runs=3):
    for _ in range(runs):
        tbl = DeltaTable(table_path).to_pyarrow_table(
            filters=[("user_id", "=", TARGET_USER), ("kind", "=", "purchase")]
        )
    ...
before = bench("BEFORE OPTIMIZE")
```

Truy vấn điểm (point query) này tìm dòng có `user_id=4242` và `kind="purchase"`. Tham số `filters=` của `to_pyarrow_table()` không chỉ là một bộ lọc sau khi đọc, nó là filter pushdown thật: delta-rs đọc thống kê min/max của từng file được lưu trong transaction log, và chỉ mở những file có khả năng chứa `user_id=4242` trước khi đọc dữ liệu thật. Đây chính là cơ chế mà Spark và Trino cũng dùng, notebook đang đo trực tiếp cơ chế đó, không phải một benchmark giả lập.

### Bước 3. Chạy OPTIMIZE và Z-ORDER

```python
TARGET_SIZE = 256 * 1024   # 256 KB
dt = DeltaTable(table_path)
dt.optimize.compact(target_size=TARGET_SIZE)
dt.optimize.z_order(["user_id"], target_size=TARGET_SIZE)
```

`compact()` nén nhiều file nhỏ thành ít file lớn hơn, kích thước mục tiêu mỗi file được giới hạn ở 256 KB (số nhỏ có chủ đích trong bài lab, ở production con số này thường là 128 đến 512 MB, điều quan trọng là tỉ lệ trước/sau, không phải kích thước tuyệt đối). `z_order(["user_id"])` sắp xếp lại dữ liệu trong quá trình viết lại file sao cho các dòng có `user_id` gần nhau về mặt không gian Z-order được đặt cạnh nhau trong cùng file, khiến khoảng min/max của `user_id` trên mỗi file trở nên hẹp và ít chồng lấp lên nhau.

### Bước 4. Đo lại tốc độ sau khi tối ưu

```python
after = bench("AFTER OPTIMIZE+ZORDER")
print(f"Speedup: {before/after:.1f}x  (target >= 3x)")
```

### Bước 5. Kiểm tra trực tiếp thống kê file để tính tỉ lệ file được bỏ qua

```python
log_dir = os.path.join(table_path, "_delta_log")
last_log = sorted(f for f in os.listdir(log_dir) if f.endswith(".json"))[-1]
with open(os.path.join(log_dir, last_log)) as fh:
    for line in fh:
        e = json.loads(line)
        if "add" in e and "stats" in e["add"]:
            stats = json.loads(e["add"]["stats"])
            mn = stats["minValues"]["user_id"]
            mx = stats["maxValues"]["user_id"]
```

Notebook mở trực tiếp file JSON commit log mới nhất và đọc trường `stats` của mỗi thao tác `add` (thêm file). Trường này chứa `minValues` và `maxValues` cho từng cột có thống kê, đây chính là dữ liệu mà công cụ truy vấn dùng để quyết định bỏ file nào. Từ đó tính được `pruned_ratio = files_after / hits`, trong đó `hits` là số file có khoảng `[min, max]` chứa giá trị `4242`.

## 4. Kết quả mong đợi

Notebook in ra hai chỉ số song song, vì slide cho phép đạt một trong hai: tốc độ tăng (speedup) từ truy vấn trước và sau tối ưu, mục tiêu ít nhất 3 lần; và tỉ lệ file bị bỏ qua (files-pruned ratio), mục tiêu ít nhất 10 lần. Speedup đo bằng đồng hồ thực (wall-clock) nên có thể nhiễu trên máy laptop đang chạy nền nhiều tiến trình khác, trong khi tỉ lệ file bị bỏ qua là một số xác định (deterministic), không phụ thuộc tải máy, nên đây là chỉ số đáng tin cậy hơn để chụp màn hình nộp bài. Notebook kết thúc bằng `NB2 complete.` nếu số file giảm sau compact, và một trong hai chỉ số đạt ngưỡng.

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 12 điểm: 3 điểm cho việc tái hiện đúng bài toán small files (ít nhất 100 file trước OPTIMIZE), 6 điểm cho việc đạt speedup từ 3 lần trở lên hoặc tỉ lệ pruning từ 10 lần trở lên, và 3 điểm cho việc số file (`numFiles`) giảm rõ rệt sau OPTIMIZE.

## 6. Sự cố thường gặp

Nếu speedup đo được nhỏ hơn 3 lần, đây là hiện tượng bình thường trên máy có RAM thấp hoặc đang chạy tải CPU nặng cùng lúc, vì đo bằng đồng hồ thực rất nhạy với tải hệ thống. Trong trường hợp này hãy dùng chỉ số files-pruned ratio, vốn được README công nhận là chỉ số thay thế hợp lệ và ổn định hơn.

## 7. Ý nghĩa kỹ thuật

Bài học quan trọng nhất ở đây là phân biệt hai thao tác có tên gần giống nhau nhưng làm hai việc khác nhau: compact chỉ nén số lượng file (giải quyết chi phí mở file và chi phí request tới object storage), còn Z-order sắp xếp lại nội dung để thống kê min/max trở nên có ý nghĩa cho việc bỏ file. Có compact mà không Z-order thì vẫn tốn chi phí mở nhiều file khi truy vấn điểm dù đã ít file hơn trước, vì dữ liệu bên trong các file lớn vẫn có thể chồng lấp về `user_id`. Đây chính là lý do notebook chạy cả hai bước, không chỉ một.
