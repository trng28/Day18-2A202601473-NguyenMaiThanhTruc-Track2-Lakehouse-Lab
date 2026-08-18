# Checkpoint 8: Agent Trajectory và Provenance theo EU AI Act

Notebook tương ứng: `notebooks/08_agents_provenance.py`

## 1. Mục tiêu của checkpoint

Checkpoint cuối cùng của phần nội dung chính gồm ba mảng ghép lại: dùng lakehouse làm nguồn sự thật cho những gì một agent đã làm (trajectory), mô phỏng đúng hình dạng giao thức MCP theo bản sửa đổi 2026-07-28 mà một agent dùng để chạm vào catalog của bạn, và cuối cùng là bài toán provenance (nguồn gốc dữ liệu) theo Điều 10 của EU AI Act, luật đã có hiệu lực từ ngày 2 tháng 8 năm 2026. Không có lệnh gọi LLM thật nào trong notebook này, agent loop được viết deterministic có chủ đích, để bạn học đúng hợp đồng dữ liệu (data contract), không bị nhiễu bởi hành vi ngẫu nhiên của model.

## 2. Điều kiện tiên quyết

Cần đã chạy `make data-ai` ở Checkpoint 0 để có `agent_traces` và `docs_multimodal`. Nếu chưa, notebook tự sinh lại.

## 3. Các bước triển khai

### Phần 1, Trajectory qua kiến trúc medallion

```python
silver = to_arrow(con.sql("""
    SELECT session_id AS trajectory_id, step, tool, status, reward, subject_id,
           input_tokens + output_tokens AS total_tokens, latency_ms,
           (input_tokens * 3.0 + output_tokens * 15.0) / 1e6 AS cost_usd,
           CASE WHEN CAST(substr(session_id, 6) AS INT) < 150
                THEN 'policy-v2' ELSE 'policy-v3' END AS agent_version
    FROM bronze_traces
"""))
write_deltalake(SILVER, silver, mode="overwrite", partition_by=["agent_version"])
```

Một trajectory (rollout) là một chuỗi (quan sát, hành động, thưởng) từ trạng thái ban đầu tới lúc kết thúc, đây là nhiên liệu mà một bước cập nhật reinforcement learning cần. Khác với dữ liệu supervised, phân phối dữ liệu trajectory thay đổi liên tục khi policy được cải thiện, nên một bộ dữ liệu tĩnh sẽ trở nên vô dụng nhanh. Vì vậy bảng trajectory cần append liên tục, có versioning, và có thể lọc theo phiên bản policy, đó là lý do Silver được phân vùng theo `agent_version`.

```python
gold = to_arrow(con.sql("""
    WITH per_traj AS (
        SELECT trajectory_id, agent_version, max(reward) AS success, count(*) AS steps,
               sum(cost_usd) AS cost_usd, sum(latency_ms) AS latency_ms,
               max(status = 'error') AS had_error
        FROM silver GROUP BY 1, 2
    )
    SELECT agent_version, count(*) AS trajectories, round(avg(success), 3) AS success_rate, ...
    FROM per_traj GROUP BY 1
"""))
```

Gold tổng hợp theo từng policy: tỉ lệ thành công, số bước trung bình, chi phí trung bình và tổng, thời gian trung bình. Đây là những chỉ số bạn cần để so sánh policy-v2 với policy-v3 trước khi quyết định rollout diện rộng.

### Bước tiếp theo, ghim phiên bản bảng vào một training run

```python
training_run = {
    "run_id": "rl-run-2026-08-17-001", "policy": "policy-v4",
    "trajectory_table": SILVER, "table_version": DeltaTable(SILVER).version(),
    "n_steps_seen": DeltaTable(SILVER).count(),
}
write_deltalake(SILVER, silver.slice(0, 400), mode="append", partition_by=["agent_version"])
pinned = DeltaTable(SILVER, version=training_run["table_version"])
```

Quy tắc mà slide đưa ra: ghim phiên bản bảng trajectory vào chính bản ghi của lần huấn luyện, cùng hợp đồng mà MLflow và Delta version thường dùng với nhau. Notebook mô phỏng đúng chuỗi thời gian thật: ghi lại số phiên bản tại thời điểm huấn luyện, sau đó "thế giới tiếp tục", có thêm dữ liệu rollout mới đổ vào bảng, và sáu tháng sau một auditor hỏi "run này thật sự đã thấy dữ liệu nào". Câu trả lời chỉ cần một số nguyên: mở lại `DeltaTable(SILVER, version=training_run["table_version"])`, số dòng đọc lại phải khớp chính xác với số đã ghi nhận lúc huấn luyện.

### Phần 2, MCP, hình dạng ranh giới giữa agent và lakehouse

```python
class LakehouseMCP:
    DESTRUCTIVE = {"drop_table", "delete_rows"}
    def tools_list(self):
        return {"tools": [...], "_meta": {"ttlMs": self.list_ttl_ms, "cacheScope": "session"}}
    def call(self, name, args=None, _meta=None):
        if name in self.DESTRUCTIVE and not _meta.get("confirmed"):
            return {"resultType": "input_required", "prompt": f"Confirm {name}(...)"}
        ...
```

Notebook không dựng một server mạng thật, mục đích là học đúng hợp đồng (contract) của bản sửa đổi MCP 2026-07-28, phần có ý nghĩa với một nền tảng dữ liệu. Năm hành vi được lập trình lại và minh chứng bằng dữ liệu thật lấy từ catalog Iceberg `nb8` vừa tạo ở phần 1:

Thứ nhất, `tools_list()` tự công bố một TTL (`ttlMs`) và phạm vi cache (`cacheScope`), cho phép một catalog có 50.000 bảng không phải liệt kê lại chính nó ở mỗi lượt hội thoại của agent.

Thứ hai, phần lõi không trạng thái (stateless core): không có bước bắt tay `initialize`, mỗi request tự mô tả mình qua trường `_meta`, cho phép một MCP server đứng sau một load balancer chia tải vòng quay mà không cần lưu session.

Thứ ba, các công việc mang tính phá hoại (`drop_table`, `delete_rows`) không được thực thi ngay, chúng trả về `resultType: "input_required"` và một câu hỏi xác nhận, chỉ thực thi khi lệnh gọi tiếp theo có `_meta={"confirmed": True}`. Agent không thể tự phê duyệt cho chính nó, cổng chặn này thuộc về giao thức, không thuộc về model.

Thứ tư, việc định tuyến qua header (`Mcp-Method`, `Mcp-Name`) cho phép một gateway định tuyến và tính phí theo từng công cụ mà không cần parse nội dung JSON.

Thứ năm, phần mở rộng `tasks`: `submit_scan` trả về một `taskId` ngay lập tức, sau đó bạn `poll` bằng `tasks_get(task_id)` cho tới khi trạng thái là `completed`, đúng hình dạng cần cho một job Spark chạy 40 phút, và cùng hình dạng mà chế độ lập kế hoạch phía server của Iceberg 1.11 trả về (`plan-id`).

Notebook chứng minh cụ thể: gọi `list_tables` năm lần liên tiếp nhưng chỉ có một lượt thật sự chạm tới catalog (`mcp.catalog_reads == 1`), nhờ cache tôn trọng TTL.

### Phần 3, Provenance theo Điều 10 EU AI Act

```python
BUCKET_SQL = """
    CASE
        WHEN license IN ('proprietary', 'commercial') THEN 'licensed'
        WHEN license = 'cc-by-4.0' THEN 'public_domain'
        WHEN license = 'user-owned' AND consent_train THEN 'scraped_optout_checked'
        WHEN license = 'synthetic' AND generator IS NOT NULL THEN 'synthetic'
        ELSE 'UNCLASSIFIED'
    END
"""
```

Nghĩa vụ dành cho hệ thống AI rủi ro cao có hiệu lực từ 2 tháng 8 năm 2026, và Điều 10 áp lên chính bộ dữ liệu huấn luyện, kiểm định, kiểm thử của bạn: nguồn gốc, cách chuẩn bị (gán nhãn, làm sạch), kiểm tra thiên lệch, và các khoảng trống dữ liệu. Slide đưa ra một cách đóng khung lại vấn đề rất thực dụng: mỗi dòng dữ liệu huấn luyện phải quy về đúng một trong bốn nhóm, licensed (có bản quyền), public_domain (thuộc phạm vi công cộng), scraped_optout_checked (thu thập nhưng đã kiểm tra quyền từ chối), synthetic (dữ liệu tổng hợp có ghi rõ công cụ sinh ra nó). Bốn nhóm đó chỉ là một cột được quản trị cộng một khoá phân vùng, không phải một trang tài liệu nội bộ rời rạc. Bất cứ dòng nào không rơi vào bốn nhóm trên bị gán `UNCLASSIFIED`, và `UNCLASSIFIED` phải là một phát hiện cần kiểm toán, tuyệt đối không được để nó âm thầm trở thành nhóm mặc định.

```python
write_deltalake(GOVERNED, governed, mode="overwrite", partition_by=["provenance_bucket"])
trainable = con.sql("SELECT count(*) FROM governed WHERE provenance_bucket <> 'UNCLASSIFIED'").fetchone()[0]
```

Khi provenance trở thành một khoá phân vùng thật, câu lệnh "loại trừ mọi thứ chúng ta không thể bảo vệ được trước kiểm toán" trở thành một phép prune phân vùng, không còn là một lượt quét toàn bảng và cầu mong may rủi.

```python
model_card = {
    "model": "vinuni-rag-v1", "corpus_table": "silver.training_corpus_governed",
    "corpus_version": corpus_version, "rows_used": trainable,
    "buckets_used": [...], "excluded_rows": governed.num_rows - trainable,
    "exclusion_reason": "license=unknown -> fails Art. 10 origin requirement",
}
```

Câu hỏi kiểu Annex IV, "corpus phiên bản nào đã dùng để huấn luyện model X", được trả lời bằng đúng ba sự kiện: `DESCRIBE HISTORY`, số phiên bản đã ghim, và mã định danh của lần chạy huấn luyện.

### Bước cuối, quyền được xoá và điểm căng giữa xoá và time travel

```python
dt = DeltaTable(GOVERNED)
dt.delete(f"subject_id = '{SUBJECT}'")
```

Luật Bảo vệ dữ liệu cá nhân của Việt Nam (Nghị định/Luật số 91/2025) và GDPR đều trao quyền yêu cầu xoá dữ liệu cho chủ thể dữ liệu. Câu hỏi khó không phải là "có xoá được không", mà là "có chứng minh được dữ liệu đó đã từng được dùng vào việc gì, kể cả trong corpus đã huấn luyện model, hay không". Vì provenance đã là một cột thật, notebook trả lời được ngay: nhóm provenance nào đã dùng dữ liệu của `user_007` trước khi xoá.

Notebook cũng chỉ rõ một điểm căng cố ý không che giấu: sau khi xoá, phiên bản cũ của bảng (trước khi xoá) vẫn còn chứa các dòng đã bị xoá thông qua time travel, vì đó chính là bản chất của time travel. Việc xoá chỉ hoàn tất thật sự khi retention hết hạn (chính là Job 3 ở Checkpoint 6). "Chúng ta hỗ trợ time travel" và "chúng ta tuân thủ quyền được xoá" mâu thuẫn trực tiếp với nhau, trừ khi cửa sổ retention là một quyết định được viết ra rõ ràng, không phải một giá trị mặc định bị bỏ quên.

## 4. Kết quả mong đợi

Notebook kết thúc bằng mười điều kiện: Silver có đúng 2 phân vùng `agent_version`, Gold có đủ dữ liệu cho cả hai policy, việc replay tại phiên bản đã ghim khớp chính xác với số bước đã ghi nhận lúc huấn luyện, năm lượt gọi `list_tables` chỉ tạo một lượt chạm catalog thật, lệnh phá hoại yêu cầu xác nhận trước khi chạy, lệnh đã xác nhận thì thực thi được, task được submit rồi poll tới trạng thái hoàn tất, đủ cả bốn nhóm Điều 10 tồn tại dưới dạng phân vùng, có ít nhất một dòng UNCLASSIFIED được phát hiện, và số dòng của chủ thể yêu cầu xoá bằng 0 sau khi xoá. Kết thúc bằng `NB8 complete.`

## 5. Tiêu chí đạt (theo rubric)

Theo `rubric.md`, checkpoint này chiếm 11 điểm: 3 điểm cho trajectory đi qua đúng medallion với Silver phân vùng theo `agent_version` và Gold phủ cả hai policy, 3 điểm cho việc ghim phiên bản và replay khớp chính xác, 3 điểm cho việc thể hiện đúng mặt giao thức MCP (cache list, xác nhận trước khi phá hoại, task hoàn tất qua polling), và 2 điểm cho việc đủ bốn nhóm Điều 10 dưới dạng phân vùng cộng việc loại UNCLASSIFIED khỏi tập dữ liệu huấn luyện.

## 6. Sự cố thường gặp

Nếu số lượt chạm catalog thật lớn hơn 1 sau năm lượt gọi `list_tables`, hãy kiểm tra lại tham số `list_ttl_ms` chưa bị đặt quá thấp so với thời gian bạn chạy tuần tự năm lệnh gọi trong một ô notebook.

## 7. Ý nghĩa kỹ thuật

Bài học lớn nhất của checkpoint này là: quản trị dữ liệu (governance) hoạt động tốt nhất khi nó được biểu diễn như một cột dữ liệu và một khoá phân vùng, không phải như một chính sách viết trên giấy. Khi provenance là một cột thật, mọi câu hỏi kiểm toán trở thành một câu truy vấn SQL, và khi phiên bản bảng là một con số được ghim vào bản ghi huấn luyện, mọi câu hỏi tái lập trở thành một phép so sánh số nguyên, không còn là một câu chuyện kể lại từ trí nhớ.
