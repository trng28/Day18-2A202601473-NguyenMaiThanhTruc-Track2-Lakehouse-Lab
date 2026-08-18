# Checkpoint 7: Vector, Multimodal và những cái bẫy về Lifecycle

Notebook tương ứng: `notebooks/07_vectors_multimodal.py`

## 1. Mục tiêu của checkpoint

Parquet và Iceberg được thiết kế cho những dòng dữ liệu cỡ vài KB, đọc tuần tự theo lô. Dữ liệu AI đa phương thức (multimodal) phá vỡ cả ba giả định đó cùng lúc: một dòng có thể nặng tới vài MB (ảnh, video, âm thanh), việc truy cập trở nên ngẫu nhiên (hàng nghìn lượt đọc mỗi giây để nuôi GPU), và một GPU bị đói dữ liệu chính là tiền bị đốt. Checkpoint này đo trực tiếp những gì thật sự bị hỏng, xây dựng tìm kiếm ngữ nghĩa (semantic search) ngay trên bảng lakehouse, và kết thúc bằng con bug quan trọng nhất của cả lab: bug về vòng đời dữ liệu (lifecycle) giữa lakehouse và một vector index bên ngoài.

## 2. Điều kiện tiên quyết

Cần đã chạy `make data-ai` ở Checkpoint 0. Nếu chưa, ô đầu notebook tự gọi `generate_ai_data.main()` để sinh corpus.

## 3. Các bước triển khai

### Bước 1. So sánh lưu trữ inline và pointer trên cùng 200 khung hình

```python
inline_tbl = pa.table({"doc_id": ..., "topic": ..., "blob": pa.array(blobs, pa.binary())})
pointer_tbl = pa.table({"doc_id": ..., "topic": ..., "blob_uri": [f"blobs/{f.name}" ...]})
write_deltalake(INLINE, inline_tbl, mode="overwrite")
write_deltalake(POINTER, pointer_tbl, mode="overwrite")
```

Hai cách bố trí cùng lưu 200 khung hình media: `inline` đặt bytes thô ngay trong một cột `blob` của bảng, `pointer` chỉ lưu một chuỗi `blob_uri`, còn bytes thật nằm ở các file riêng bên ngoài. Tổng số byte lưu trữ là gần như giống nhau ở cả hai cách, khác biệt không nằm ở dung lượng, nó nằm ở việc một reader bị buộc phải chạm vào bao nhiêu dữ liệu.

### Bước 2. Chứng minh cột blob không tốn gì cho truy vấn phân tích

```python
def column_bytes(table_path):
    md = pq.ParquetFile(data_file).metadata
    for rg in range(md.num_row_groups):
        for c in range(md.row_group(rg).num_columns):
            col = md.row_group(rg).column(c)
            out[col.path_in_schema] += col.total_compressed_size
```

Một truy vấn kiểu `SELECT topic, count(*) GROUP BY topic` không nhắc gì tới cột `blob`. Vì Parquet là định dạng cột (columnar), engine chỉ đọc các column chunk cần thiết. Notebook chứng minh điều này trực tiếp từ metadata footer của file Parquet, không cần đo thời gian, cột blob gần như không tốn gì cho loại truy vấn này. Đây là bằng chứng cho thấy lời khuyên phổ biến "không bao giờ để blob trong bảng" không hoàn toàn đúng cho trường hợp quét phân tích.

### Bước 3. Đo khuếch đại khi truy cập ngẫu nhiên một dòng

```python
n_rg, rg_rows, rg_bytes = row_group_bytes(INLINE)
one_blob = len(blobs[0])
AMPLIFICATION = rg_bytes / one_blob
```

Đơn vị đọc/ghi (I/O) của Parquet là row group, không phải từng dòng riêng lẻ. Để lấy một blob, bạn phải đọc và giải nén nguyên row group chứa nó. Với cách lưu pointer, bạn chỉ cần một lệnh GET đúng số byte cần. Đây chính là con số mà tuyên bố "3 đến 35 lần nhanh hơn khi truy cập ngẫu nhiên" của định dạng Lance thực chất đang nói tới, và notebook yêu cầu bạn đo nó ra một con số cụ thể trên chính dữ liệu của mình, mục tiêu tối thiểu 5 lần.

### Bước 4. Lượng tử hoá embedding sang int8

```python
SCALE = 127.0
emb_i8 = np.clip(np.round(emb * SCALE), -127, 127).astype("int8")
```

Embedding vốn là vector đơn vị (unit vector) nên toạ độ nằm trong khoảng [-1, 1]. Phép lượng tử hoá đối xứng (symmetric quantization) này nhân toạ độ với 127 rồi làm tròn về số nguyên 8-bit có dấu. Một embedding 768 chiều lưu ở float32 tốn 3.072 byte mỗi dòng, ở int8 chỉ tốn 768 byte, đúng 4 lần nhỏ hơn. Notebook viết cả hai phiên bản ra hai bảng Delta riêng và đo trực tiếp dung lượng thật trên đĩa, mục tiêu tối thiểu 3 lần nhỏ hơn (Parquet nén khiến tỉ lệ thật khác một chút so với lý thuyết 4 lần).

### Bước 5. Tìm kiếm ngữ nghĩa bằng một câu SQL DuckDB

```python
hits = con.sql(f"""
    SELECT doc_id, title, topic,
           array_cosine_similarity(emb::FLOAT[{dim}], {query_vec}::FLOAT[{dim}]) AS sim
    FROM docs ORDER BY sim DESC LIMIT 5
""").fetchall()
```

DuckDB có hàm `array_cosine_similarity` ngay trong core, không cần extension, không cần tải qua mạng. Có một điểm cần lưu ý bắt buộc: bảng được viết ra với kiểu `fixed_size_list<float>[256]`, nhưng giao thức Delta không có kiểu vector cố định chiều, chỉ có `list<element>` biến chiều, nên khi đọc lại, cột `emb` trở thành `list<float>` biến chiều, và bạn phải ép kiểu về `FLOAT[dim]` trước khi các hàm mảng cố định chiều của DuckDB chịu chạy. Đây chính là lý do Hudi 1.2 thêm hẳn một kiểu cột hạng nhất `VECTOR(dim, type)`, và slide gọi đây là xu hướng của năm 2026.

Notebook còn dựng thêm một truy vấn kết hợp vector với cột nguồn gốc dữ liệu (`WHERE consent_train AND license <> 'unknown'`), nhấn mạnh một lợi thế của lakehouse so với vector database độc lập: "tìm tài liệu tương tự mà chúng ta thật sự được phép huấn luyện trên đó" chỉ là một câu truy vấn duy nhất, vì vector và cột quản trị dữ liệu nằm cùng một dòng.

### Bước 6. Đo recall và độ trung thực chủ đề của int8

```python
gold = topk(emb, emb[q_idx])
cand = topk(emb_i8_deq, emb_i8_deq[q_idx])
recall = np.mean([len(set(g) & set(c)) / len(g) for g, c in zip(gold, cand)])
topic_fidelity = np.mean([(topics_arr[c] == topics_arr[q]).mean() for q, c in zip(q_idx, cand)])
```

4 lần nhỏ hơn về dung lượng chỉ có giá trị nếu chất lượng truy hồi không sụp đổ. Notebook đo hai chỉ số: `recall@10` theo đúng ID tài liệu (nghiêm khắc, một lần đổi chỗ giữa hai tài liệu tương đương về ý nghĩa cũng bị tính là trượt), mục tiêu tối thiểu 0.80, và độ trung thực chủ đề (topic fidelity, tỉ lệ kết quả top-10 vẫn đúng chủ đề với câu truy vấn), mục tiêu tối thiểu 0.95. Với ứng dụng RAG, độ trung thực chủ đề thường là chỉ số phản ánh sát hơn chất lượng thực tế, vì phần lớn các trường hợp "trượt" theo recall theo ID chỉ là hoán đổi giữa hai tài liệu gần tương đương nhau.

### Bước 7. Tái hiện bug về vòng đời dữ liệu

```python
EXTERNAL = path("scratch", "vector_index_external")
write_deltalake(EXTERNAL, pa.table({"doc_id": ..., "emb": ...}), mode="overwrite")
```

Đầu tiên, notebook mô phỏng một job đồng bộ hàng đêm tạo ra một vector index bên ngoài, tức một bản sao của embedding.

```python
SUBJECT = "user_042"
dt = DeltaTable(INTABLE)
victim_ids = con.sql(f"SELECT doc_id FROM docs WHERE subject_id = '{SUBJECT}'").fetchall()
dt.delete(f"subject_id = '{SUBJECT}'")
```

Sau đó, `user_042` thực hiện quyền yêu cầu xoá dữ liệu (right to erasure). Notebook xoá đúng chỗ, tức xoá khỏi lakehouse, hệ thống được coi là nguồn sự thật (system of record). Nhưng vector index bên ngoài không bị chạm tới.

```python
in_hits = con.sql(f"SELECT count(*) FROM intable WHERE doc_id IN (...)").fetchone()[0]
ex_hits = con.sql(f"SELECT count(*) FROM external WHERE doc_id IN (...)").fetchone()[0]
```

Kết quả: số dòng của các tài liệu đã xoá còn lại trong lakehouse bằng 0, đúng như kỳ vọng, nhưng số dòng còn lại trong vector index bên ngoài lớn hơn 0, đây là một vi phạm thật. Vector index bên ngoài sẽ tiếp tục trả về nội dung của `user_042` cho một prompt RAG cho đến lần đồng bộ tiếp theo, và nếu lần đồng bộ đó chỉ là một upsert một chiều (trường hợp phổ biến), nó sẽ không bao giờ biết phải xoá, vì các pipeline đồng bộ thường quên mất chính thao tác xoá.

### Bước 8. Cơ chế lan truyền đúng: Change Data Feed

```python
write_deltalake(CDF_TABLE, ..., configuration={"delta.enableChangeDataFeed": "true"})
DeltaTable(CDF_TABLE).delete(f"subject_id = '{SUBJECT}'")
cdf = DeltaTable(CDF_TABLE).load_cdf(starting_version=1).read_all()
```

Nếu bạn buộc phải giữ một index dẫn xuất bên ngoài, đừng đồng bộ lại toàn bảng, hãy đọc change feed để các thao tác xoá được lan truyền như sự kiện hạng nhất. `_change_type` trong CDF sẽ có giá trị `delete` cho đúng các dòng đã xoá, index bên ngoài chỉ cần lắng nghe sự kiện này thay vì phải tự đoán "vector này còn hợp lệ không". Lựa chọn tốt nhất vẫn là không cần đồng bộ gì cả, giữ vector ngay trong dòng dữ liệu như Bước 4 đã làm, khi đó vòng đời được chính bảng đảm bảo tự động.

## 4. Kết quả mong đợi

Notebook kết thúc bằng bảy điều kiện: khuếch đại truy cập ngẫu nhiên tối thiểu 5 lần, int8 nhỏ hơn tối thiểu 3 lần trên đĩa, recall@10 tối thiểu 0.80, độ trung thực chủ đề tối thiểu 0.95, top-5 kết quả tìm kiếm chia sẻ đúng chủ đề với câu truy vấn, bug vòng đời tái hiện đúng (0 lượt trúng trong bảng, lớn hơn 0 lượt trúng ở index ngoài), và change feed phát ra đúng số sự kiện xoá bằng đúng số tài liệu bị xoá. Kết thúc bằng `NB7 complete.`

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 13 điểm: 4 điểm cho khuếch đại truy cập ngẫu nhiên đo được và giải thích được bằng khái niệm row group, 4 điểm cho lượng tử hoá int8 nhỏ hơn tối thiểu 3 lần cộng cả recall@10 và độ trung thực chủ đề được báo cáo, 1 điểm cho việc tìm kiếm ngữ nghĩa chạy được như SQL và trả về hàng xóm cùng chủ đề, và 4 điểm cho việc tái hiện đúng bug vòng đời dữ liệu.

## 6. Sự cố thường gặp

Nếu bạn gặp lỗi `No function matches array_cosine_similarity(FLOAT[], ...)`, nguyên nhân hầu như luôn là thiếu bước ép kiểu `emb::FLOAT[dim]`, vì Delta trả cột vector về dưới dạng list biến chiều như đã giải thích ở Bước 5.

## 7. Ý nghĩa kỹ thuật

Bài học quan trọng nhất của checkpoint này không phải là kỹ thuật lượng tử hoá, mà là câu trích trong chính notebook: khi một dòng bị xoá, hết hạn, được sửa hoặc xử lý lại, embedding của nó phải tuân theo đúng vòng đời đó, và mọi pipeline đồng bộ từ warehouse sang vector database riêng biệt đều là một lỗi lệch vòng đời (lifecycle-skew bug) đang chờ xảy ra, và ngay khi có một yêu cầu quyền được quên (right to forget), nó trở thành một lỗi tuân thủ pháp lý (compliance bug), không còn là lỗi kỹ thuật đơn thuần nữa.
