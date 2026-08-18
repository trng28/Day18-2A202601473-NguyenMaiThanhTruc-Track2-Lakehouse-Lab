# Checkpoint 0: Setup và chuẩn bị dữ liệu

## 1. Mục tiêu của checkpoint

Checkpoint này không có notebook riêng. Nhiệm vụ của nó là đưa máy của bạn từ trạng thái "vừa clone repo" sang trạng thái "sẵn sàng chạy cả tám notebook mà không gặp lỗi thiếu môi trường hoặc thiếu dữ liệu". Toàn bộ lab chạy hoàn toàn offline: không cần API key, không cần Docker, không cần JVM, không tải model, không tải extension DuckDB qua mạng. Nếu Checkpoint 0 chạy đúng, mọi checkpoint phía sau chỉ còn là vấn đề đọc hiểu, không còn là vấn đề môi trường.

## 2. Điều kiện tiên quyết

Máy cần có Python phiên bản từ 3.10 đến 3.14. Không cần cài thêm Spark, không cần Docker, không cần tài khoản cloud. Nếu máy có sẵn công cụ `uv`, quá trình cài đặt sẽ nhanh hơn (khoảng 4 giây so với khoảng 20 giây khi dùng `pip` thuần).

## 3. Các bước triển khai

### Bước 1. Lấy mã nguồn

```bash
git clone https://github.com/VinUni-AI20k/Day18-Track2-Lakehouse-Lab.git
cd Day18-Track2-Lakehouse-Lab
```

### Bước 2. Tạo virtual environment và cài thư viện

```bash
make setup
```

Lệnh này thực hiện ba việc liên tiếp. Thứ nhất, nó tạo một virtual environment tại `.venv` (dùng `uv venv` nếu có, nếu không sẽ rơi về `python3 -m venv`). Thứ hai, nó kiểm tra phiên bản Python nằm trong khoảng 3.10 đến 3.14, nếu không thoả sẽ báo lỗi rõ ràng thay vì để cài đặt thất bại nửa chừng. Thứ ba, nó cài `requirements.txt` vào venv đó. Gói quan trọng nhất là `deltalake>=1.0,<2.0` (bản delta-rs 1.x, có `load_cdf()`, `repair()` và `compact_logs()`), `pyiceberg[sql-sqlite,pyarrow]>=0.9,<1.0` cho catalog Iceberg cục bộ, `duckdb>=1.1,<2.0` cho công cụ truy vấn SQL, và `polars`, `pyarrow`, `numpy` cho xử lý dữ liệu. Tổng dung lượng cài đặt khoảng 180 MB.

Bước cuối của `make setup` là đồng bộ notebook: nó gọi `jupytext --to notebook --update notebooks/*.py` để sinh ra các file `.ipynb` tương ứng từ các file `.py` (định dạng Jupytext `py:percent`). Notebook trong repo được lưu dưới dạng `.py` có chủ đích, vì file `.py` nhẹ và review được trong Git, còn `.ipynb` chỉ là bản dựng cục bộ.

### Bước 3. Chạy smoke test

```bash
make smoke
```

Lệnh này gọi `scripts/verify_lite.py`, một script kiểm tra khoảng chín khả năng mà tám notebook sẽ dùng đến, tất cả đều chạy offline trong dưới 15 giây. Các nhóm kiểm tra gồm: ghi và đọc một bảng Delta, time travel và `history()`, các hàm bảo trì `optimize.compact()` và `vacuum()` dùng cho Checkpoint 6, Change Data Feed dùng cho Checkpoint 7, catalog Iceberg với hidden partitioning dùng cho Checkpoint 5, tìm kiếm vector bằng hàm lõi `array_cosine_similarity` của DuckDB, và việc DuckDB đọc bảng Delta qua Arrow thay vì qua extension `delta_scan()` tải từ mạng. Nếu `make smoke` chạy xanh, gần như chắc chắn cả lab sẽ chạy được trên máy đó, kể cả trong phòng học bị chặn mạng.

### Bước 4. Sinh dữ liệu cho Checkpoint 4

```bash
make data
```

Lệnh này chạy `scripts/generate_data_lite.py`, sinh 200.000 dòng dữ liệu giả lập các lượt gọi LLM (log quan sát LLM) vào tầng Bronze, tại đường dẫn `_lakehouse/bronze/llm_calls_raw`. Có ba lựa chọn thiết kế đáng chú ý trong generator này mà bạn nên hiểu trước khi sang Checkpoint 4.

Thứ nhất, timestamp được rải đều trên 7 ngày UTC bắt đầu từ 2026-04-01, để tầng Gold ở Checkpoint 4 có đủ 7 ngày dữ liệu, nhân với 3 model, tạo ra 21 dòng tổng hợp có ý nghĩa để phân tích.

Thứ hai, khoảng 5% các `request_id` bị lặp lại một cách chủ ý (mô phỏng hành vi retry khi gọi API), để bước khử trùng lặp ở tầng Silver có cái để chứng minh: số dòng Silver phải nhỏ hơn số dòng Bronze.

Thứ ba, độ trễ (latency) được tính dựa trên số token đầu ra theo từng model (`claude-haiku-4-5`, `claude-sonnet-4-6`, `claude-opus-4-7`), có giới hạn hợp lý để trông giống một dịch vụ LLM thật, không phải một pipeline bị treo.

Nếu bạn quên chạy `make data` và mở thẳng notebook Checkpoint 4, đừng lo, notebook đó có cơ chế tự sinh dữ liệu Bronze nếu phát hiện đường dẫn chưa tồn tại.

### Bước 5. Sinh dữ liệu cho Checkpoint 7 và Checkpoint 8

```bash
make data-ai
```

Lệnh này chạy `scripts/generate_ai_data.py`, sinh ra ba nhóm dữ liệu. Nhóm thứ nhất là corpus tài liệu đa phương thức tại `_lakehouse/bronze/docs_multimodal`, gồm 2.000 tài liệu, mỗi tài liệu có một embedding 256 chiều được sinh theo cấu trúc chủ đề (centroid theo 8 topic cộng nhiễu), để tìm kiếm ngữ nghĩa ở Checkpoint 7 trả về kết quả có ý nghĩa thật, không phải một phép so khớp ngẫu nhiên. Mỗi tài liệu còn mang theo các cột nguồn gốc dữ liệu (source, license, consent_train, generator, subject_id) ngay từ lúc sinh ra, vì đó chính là bài học của Checkpoint 8: thông tin nguồn gốc không được thu thập từ lúc nạp dữ liệu thì sau này không thể dựng lại được.

Nhóm thứ hai là 200 file blob nhị phân 64 KB mỗi file tại `_lakehouse/blobs`, mô phỏng các khung hình media dùng cho phần so sánh lưu trữ inline và pointer ở Checkpoint 7.

Nhóm thứ ba là 300 phiên agent (agent trajectories) tại `_lakehouse/bronze/agent_traces`, mỗi phiên có từ 2 đến 8 bước, khoảng 72% phiên thành công, dùng cho Checkpoint 8.

Giống Checkpoint 4, các notebook 7 và 8 cũng tự sinh dữ liệu này nếu bạn quên chạy lệnh trên.

### Bước 6. Mở Jupyter Lab

```bash
make lab
```

Lệnh này đồng bộ lại `.ipynb` từ `.py` (nếu bạn vừa sửa notebook `.py` bằng tay) rồi mở Jupyter Lab tại `http://localhost:8888` với thư mục làm việc là `notebooks/`, không yêu cầu token đăng nhập.

## 4. Vị trí lưu dữ liệu và lưu ý trên WSL

Theo mặc định, mọi dữ liệu sinh ra nằm dưới `_lakehouse/` cạnh repo, được điều khiển bởi biến môi trường `LAKEHOUSE_ROOT` trong `scripts/lakehouse.py`. Nếu bạn chạy lab trong WSL và repo đang nằm trên một ổ đĩa Windows được mount qua `/mnt/*` (ví dụ `/mnt/d/...`), việc ghi bảng Delta có thể thất bại ngẫu nhiên với lỗi `Generic LocalFileSystem error: Upload aborted`, vì driver ghi file cục bộ của delta-rs không hoạt động ổn định trên hệ thống mount 9p của WSL. `scripts/lakehouse.py` đã có sẵn cơ chế phát hiện trường hợp này: nếu bạn không tự đặt `LAKEHOUSE_ROOT` và repo đang ở dưới `/mnt/`, dữ liệu Delta sẽ tự động được chuyển sang `~/.cache/day18-lakehouse/_lakehouse` trên hệ thống file Linux gốc, và một dòng thông báo `[lakehouse] Repo is on a WSL /mnt mount...` sẽ in ra khi bạn chạy `make data` lần đầu. Nếu bạn muốn chỉ định vị trí khác, đặt biến `LAKEHOUSE_ROOT` trước khi chạy lệnh `make`.

## 5. Kết quả mong đợi

Sau khi hoàn thành cả sáu bước trên, bạn sẽ có: một virtual environment hoạt động tại `.venv`, `make smoke` in ra chín dòng `✓` và kết thúc bằng thông báo lab đã sẵn sàng, thư mục `_lakehouse/bronze/llm_calls_raw` chứa 200.000 dòng, thư mục `_lakehouse/bronze/docs_multimodal` và `_lakehouse/bronze/agent_traces` đã tồn tại, và Jupyter Lab mở được trong trình duyệt với tám notebook nhìn thấy trong thư mục `notebooks/`.

## 6. Sự cố thường gặp

Nếu `make setup` báo `python3: command not found`, hãy cài Python 3.10 đến 3.14, hoặc cài công cụ `uv` để nó tự tải một bản Python phù hợp.

Nếu bạn thấy lỗi `AttributeError: 'DeltaTable' object has no attribute 'files'`, virtual environment của bạn đang dùng `deltalake` bản 0.x cũ. Chạy `make clean && make setup` để cài lại đúng bản 1.x mà lab yêu cầu (bản này dùng `file_uris()` thay cho `files()`).

Nếu bạn quên chạy `make data` hoặc `make data-ai` trước khi mở notebook, không sao, các notebook Checkpoint 4, 7, 8 tự phát hiện dữ liệu thiếu và tự sinh lại.

Nếu bạn mở nhiều notebook cùng lúc trong Jupyter, điều này an toàn: các notebook dùng catalog Iceberg (Checkpoint 5, 6, 8) và cả `make smoke` đều dùng một thư mục catalog riêng cho từng notebook, nên chúng không xoá dữ liệu của nhau.

## 7. Ý nghĩa của checkpoint này

Checkpoint 0 không dạy một khái niệm lakehouse cụ thể, nhưng nó thiết lập một nguyên tắc quan trọng chạy suốt cả lab: mọi thứ phải tái lập được (reproducible) và tự phục hồi được (self-healing) khi thiếu một bước tiên quyết. Đây chính là tinh thần mà một hệ thống lakehouse thật sự cần có ở quy mô production, nơi một pipeline không nên sụp đổ chỉ vì một bước chuẩn bị dữ liệu bị bỏ sót.
