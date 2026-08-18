# Checkpoint 4: Kiến trúc Medallion cho AI Observability

Notebook tương ứng: `notebooks/04_medallion.py`

## 1. Mục tiêu của checkpoint

Checkpoint này lắp ráp lại đúng kiến trúc medallion Bronze, Silver, Gold trên một trường hợp sử dụng thật: quan sát các lượt gọi LLM (LLM observability). Đây là hiện thân của bài tập lớn (Milestone 1) mà slide §8 mô tả, và là notebook đầu tiên trong lab chuỗi ba tầng đầy đủ từ dữ liệu thô đến số liệu tổng hợp có thể đưa vào dashboard.

## 2. Điều kiện tiên quyết

Cần đã chạy `make data` ở Checkpoint 0 để có dữ liệu Bronze. Nếu bạn bỏ qua bước đó, ô đầu tiên của notebook tự phát hiện đường dẫn Bronze chưa tồn tại và tự gọi `generate_data_lite.main()` để sinh dữ liệu, tránh việc bạn gặp một lỗi Rust thô khó hiểu như `Os {{ code: 2, kind: NotFound }}`.

## 3. Các bước triển khai

### Bước 1. Xác định ba đường dẫn tầng

```python
BRONZE = path("bronze", "llm_calls_raw")
SILVER = path("silver", "llm_calls")
GOLD   = path("gold",   "llm_daily_metrics")
if not Path(BRONZE).exists():
    import generate_data_lite
    generate_data_lite.main()
```

### Bước 2. Kiểm tra tầng Bronze

```python
bronze_n = DeltaTable(BRONZE).to_pyarrow_table().num_rows
```

Tầng Bronze chứa dữ liệu thô, mỗi dòng có một `request_id`, một `ts`, và một cột `raw_json` là chuỗi JSON thô chứa các trường như model, usage, latency_ms, status. Đây đúng là hình dạng dữ liệu log thật của một hệ thống gọi LLM: chưa được chuẩn hoá, có thể có bản ghi lặp do retry.

### Bước 3. Xây tầng Silver bằng một câu SQL DuckDB

```python
con.register("bronze", DeltaTable(BRONZE).to_pyarrow_table())
silver_arrow = con.sql("""
    WITH parsed AS (
      SELECT request_id, ts, CAST(ts AS DATE) AS date,
             json_extract_string(raw_json, '$.model') AS model,
             json_extract_string(raw_json, '$.user_id') AS user_id,
             CAST(json_extract(raw_json, '$.usage.input')  AS INTEGER) AS prompt_tokens,
             CAST(json_extract(raw_json, '$.usage.output') AS INTEGER) AS completion_tokens,
             CAST(json_extract(raw_json, '$.latency_ms')   AS INTEGER) AS latency_ms,
             json_extract_string(raw_json, '$.status') AS status,
             ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn
      FROM bronze
    )
    SELECT request_id, ts, date, model, user_id, prompt_tokens, completion_tokens, latency_ms, status
    FROM parsed WHERE rn = 1 AND model IS NOT NULL
""").arrow()
write_deltalake(SILVER, silver_arrow, mode="overwrite", partition_by=["date"])
```

Câu SQL này làm ba việc trong một lần quét: trích xuất các trường từ JSON thô và ép kiểu đúng (parse), loại bỏ các dòng có model rỗng (validate), và khử trùng lặp theo `request_id` bằng cách đánh số thứ tự `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)` rồi chỉ giữ dòng có `rn = 1`, tức là bản ghi sớm nhất theo thời gian cho mỗi `request_id`. Vì Bronze có khoảng 5% `request_id` bị lặp do retry (xem Checkpoint 0), phép khử trùng lặp này chắc chắn làm giảm số dòng. Bảng Silver được viết ra có phân vùng theo `date`, một lựa chọn hợp lý vì phần lớn truy vấn phân tích log sẽ lọc theo ngày.

```python
assert silver_n < bronze_n
```

Notebook tự kiểm tra ngay tại đây rằng Silver phải có ít dòng hơn Bronze, nếu không, đây là dấu hiệu bạn đang dùng dữ liệu Bronze cũ (sinh trước khi generator có cơ chế tiêm bản ghi lặp).

### Bước 4. Xây tầng Gold bằng cách join với bảng giá token

```python
COST_TABLE = """
  VALUES ('claude-haiku-4-5', 0.80, 4.00),
         ('claude-sonnet-4-6', 3.00, 15.00),
         ('claude-opus-4-7', 15.00, 75.00)
"""
gold_arrow = con.sql(f"""
    WITH cost(model, c_in, c_out) AS ({COST_TABLE})
    SELECT s.date, s.model,
           QUANTILE_CONT(s.latency_ms, 0.50) AS p50_latency_ms,
           QUANTILE_CONT(s.latency_ms, 0.95) AS p95_latency_ms,
           SUM(s.prompt_tokens) AS total_prompt_tokens,
           SUM(s.completion_tokens) AS total_completion_tokens,
           AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
           (SUM(s.prompt_tokens) * c.c_in / 1e6) + (SUM(s.completion_tokens) * c.c_out / 1e6) AS cost_usd
    FROM silver s JOIN cost c USING (model)
    GROUP BY s.date, s.model, c.c_in, c.c_out
""").arrow()
write_deltalake(GOLD, gold_arrow, mode="overwrite", partition_by=["date"])
DeltaTable(GOLD).optimize.z_order(["model"])
```

Bảng `cost` ở đây chỉ là một bảng giá minh hoạ (không phải giá niêm yết thật, mục đích là dạy kỹ thuật, không phải để trích dẫn làm chuẩn giá). Điểm quan trọng cần hiểu là `QUANTILE_CONT(latency_ms, 0.50)` và `0.95` tính p50 và p95, hai chỉ số chuẩn để mô tả độ trễ dịch vụ (trung vị và đuôi phân phối), thay vì chỉ dùng trung bình (average) vốn dễ bị lệch bởi các outlier. `error_rate` được tính bằng tỉ lệ dòng có `status <> 'ok'`. Cuối cùng bảng Gold được Z-order theo `model`, vì phần lớn dashboard sẽ lọc hoặc nhóm theo model.

### Bước 5. Xác minh tầng Gold đạt độ phủ

```python
n_dates = gold_df.select("date").n_unique()
n_models = gold_df.select("model").n_unique()
assert n_dates >= 7
```

## 4. Kết quả mong đợi

Cả ba tầng `_lakehouse/bronze/llm_calls_raw`, `_lakehouse/silver/llm_calls`, `_lakehouse/gold/llm_daily_metrics` tồn tại trên đĩa. Số dòng Silver nhỏ hơn số dòng Bronze (bằng chứng khử trùng lặp có hoạt động thật, không phải chạy qua). Bảng Gold có ít nhất 7 ngày khác nhau nhân với 3 model, tức tối thiểu 21 dòng, mỗi dòng có đầy đủ p50, p95, tổng token, cost_usd và error_rate không rỗng.

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 12 điểm: 4 điểm cho việc cả ba tầng đều hiện diện trên lớp lưu trữ, 4 điểm cho việc Silver giảm dòng đo được rõ ràng so với Bronze, và 4 điểm cho việc Gold tính đúng p50/p95, cost_usd, error_rate trên tối thiểu 7 ngày nhân 3 model.

## 6. Sự cố thường gặp

Nếu Gold có ít hơn 7 ngày, hãy chạy lại `make data` bằng bản sinh dữ liệu mới nhất trong repo, generator hiện tại rải dữ liệu đều trên đúng 7 ngày UTC. Nếu bạn thấy `error_rate` luôn bằng 0, hãy kiểm tra lại việc parse trường `status` từ JSON, có thể bạn đã sửa câu SQL và làm mất điều kiện `status <> 'ok'`.

## 7. Ý nghĩa kỹ thuật

Notebook này cho thấy rõ vì sao mô hình medallion không chỉ là ba cái tên đẹp: mỗi tầng giải quyết một nhiệm vụ khác nhau và không thể gộp chung. Bronze bảo toàn dữ liệu thô để có thể xử lý lại (replay) nếu logic Silver sau này thay đổi. Silver là nơi duy nhất chịu trách nhiệm làm sạch và khử trùng lặp, và vì nó là một bảng Delta thật, bước khử trùng lặp này chỉ cần chạy lại (idempotent) mà không sợ nhân đôi dữ liệu. Gold là tầng duy nhất được phép mang theo giả định nghiệp vụ (giá token, ngưỡng lỗi), tách khỏi Silver để khi giá token thay đổi, bạn chỉ cần chạy lại phần tổng hợp, không cần đụng vào toàn bộ pipeline làm sạch.
