# Checkpoint 9: Kiểm thử và Nộp bài

## 1. Mục tiêu của checkpoint

Đây là checkpoint đóng gói: xác nhận toàn bộ tám notebook chạy xanh từ một môi trường sạch, và chuẩn bị đúng những gì cần nộp theo `rubric.md`. Mọi tiêu chí trong rubric đều có thể kiểm tra được bằng máy (machine-checkable), vì mỗi notebook tự kết thúc bằng một khối `assert` trên chính tiêu chí đạt của nó, nên nếu `make run-all` chạy xanh, phần cơ học của rubric đã được thoả mãn, phần còn lại thuộc về khả năng giải thích số liệu của bạn.

## 2. Điều kiện tiên quyết

Đã hoàn thành Checkpoint 0 đến Checkpoint 8, tức đã chạy qua và hiểu cả tám notebook ít nhất một lần trong Jupyter Lab.

## 3. Các bước triển khai

### Bước 1. Chạy bộ kiểm thử pytest

```bash
make test
```

Lệnh này chạy `pytest` theo cấu hình trong `pytest.ini`, thực thi các bài kiểm thử trong `tests/test_lab18.py`, tổng cộng 22 test, hoàn tất trong khoảng 1 giây. Đây là cùng cổng kiểm thử mà giảng viên chạy khi chấm bài, không phải một bộ kiểm thử riêng của bạn.

### Bước 2. Chạy toàn bộ tám notebook ở chế độ headless

```bash
make run-all
```

Lệnh này gọi `scripts/run_all.py`, lần lượt thực thi từng file trong `notebooks/*.py` bằng `subprocess.run([sys.executable, str(nb)], ...)`, đo thời gian, và in `PASS` hoặc `FAIL` cho từng notebook dựa trên mã thoát (exit code) của tiến trình. Vì mỗi notebook tự chứa khối `assert all(checks.values())` ở cuối, một notebook trả về mã thoát khác 0 nghĩa là ít nhất một tiêu chí thật sự đã không đạt, không phải một lỗi giả định. Script in ra tổng kết dạng "8/8 passed" cùng thời gian chạy, và nếu có notebook thất bại, nó in kèm 1500 ký tự cuối của cả `stdout` và `stderr` để bạn định vị lỗi nhanh.

### Bước 3 (khuyến nghị nhưng không bắt buộc). Chạy mô phỏng hành vi học viên

```bash
SIM_FAST=1 make simulate
```

Lệnh này chạy `tests/simulate_students.py`, tái hiện 12 tình huống thực tế mà học viên thường gặp nhưng người viết lab không lường tới khi phát triển: chạy notebook theo thứ tự ngược, chạy lại notebook lần thứ hai, quên chạy `make data`, chạy từ thư mục `notebooks/` (mặc định của Jupyter) thay vì thư mục gốc repo, mở hai notebook cùng lúc, chạy `make smoke` trong lúc một notebook khác đang thực thi, mất kết nối mạng hoàn toàn, máy đang chịu tải CPU nặng, thực thi qua `nbconvert` thay vì chạy trực tiếp file `.py`, chạy `make clean` giữa lúc lab đang mở, dùng Python 3.10 (bản cũ nhất được hỗ trợ), và dùng `pip` thuần không có `uv`. Tham số `SIM_FAST=1` bỏ qua hai kịch bản phải dựng lại virtual environment từ đầu, giúp chạy nhanh hơn khi bạn chỉ muốn kiểm tra lại nhanh sau một thay đổi nhỏ.

### Bước 4. Chuẩn bị các notebook đã chạy, giữ nguyên output

Trong Jupyter Lab, chạy đủ Run All cho từng notebook (hoặc dựa vào kết quả của `make run-all` ở Bước 2, vì `run_all.py` chạy trực tiếp file `.py`, bạn vẫn cần mở và Run All trong Jupyter ít nhất một lần để `.ipynb` tương ứng có lưu output). Không xoá output trước khi lưu, vì `rubric.md` yêu cầu nộp "tám notebook đã chạy, giữ output cell".

### Bước 5. Chuẩn bị bằng chứng ảnh chụp

Theo `rubric.md`, cần ít nhất một trong hai loại bằng chứng đặt tại `submission/screenshots/`: kết quả lệnh `tree _lakehouse/` cộng nội dung của một file `_delta_log/*.json` bất kỳ (đường lightweight), hoặc ảnh chụp giao diện MinIO cho thấy `_delta_log/` và cấu trúc bucket (nếu bạn có chạy thêm đường Spark/Docker).

### Bước 6. Viết REFLECTION.md

Tạo file `submission/REFLECTION.md`, không dài hơn 200 từ, trả lời câu hỏi: trong "Top 5 Lakehouse Anti-Patterns" của slide, nhóm bạn dễ vướng vào cái nào nhất, và vì sao. Câu trả lời tốt nên gắn với một số liệu cụ thể bạn vừa đo được ở một trong tám notebook, không nên chỉ là nhận xét chung.

### Bước 7 (tuỳ chọn, không tính điểm chính). Bonus Challenge

Nếu muốn làm thêm, `BONUS-CHALLENGE.md` (tiếng Việt) hoặc `BONUS-CHALLENGE-EN.md` (tiếng Anh) mô tả một bài tập mở: chọn một bài toán dữ liệu khó thật (LLM observability ở quy mô 1 tỷ request mỗi ngày, CDC tuân thủ Nghị định 13, corpus hàng nghìn tỷ token, RAG đa phương thức, tiering để chặn trần chi phí FinOps, hoặc migration catalog), viết một architecture brief bảo vệ được lựa chọn thiết kế của mình. Kết quả đặt tại `submission/bonus/ARCHITECTURE.md`, được nhận xét viết tay nhưng không tính vào 100 điểm.

### Bước 8. Fork, commit, mở Pull Request

```bash
git remote add upstream https://github.com/VinUni-AI20k/Day18-Track2-Lakehouse-Lab.git
git push origin main
```

Fork repo về tài khoản cá nhân dạng `<username>/Day18-Track2-Lakehouse-Lab`, đẩy lên nhánh của bạn tám notebook đã chạy cộng thư mục `submission/`, rồi mở một Pull Request về repo upstream với tiêu đề đúng định dạng `[NXX] Lab18 — <Họ Tên>`.

## 4. Kết quả mong đợi

`make test` in ra 22 test đều `passed`. `make run-all` in ra `8/8 passed` cùng tổng thời gian chạy (thường dưới 10 giây trên đường lightweight). Pull Request mở lên chứa đủ tám notebook có output, thư mục `submission/screenshots/` có ít nhất một bằng chứng hợp lệ, và `submission/REFLECTION.md` không vượt 200 từ.

## 5. Thang điểm tổng quan (theo rubric.md)

Tổng điểm của lab là 100, tương ứng 30% trọng số Track 2 Daily Lab. Phần A, Foundations, gồm Checkpoint 1 đến 4, chiếm 44 điểm. Phần B, Lakehouse 2026, gồm Checkpoint 5 đến 8, chiếm 50 điểm. Phần C, Reproducibility, chiếm 6 điểm, trong đó 2 điểm cho `make test` chạy xanh và 4 điểm cho `make run-all` chạy xanh từ một `make setup` sạch (tức trên một máy hoặc một môi trường vừa cài đặt lại từ đầu, không phải môi trường đã chạy dở trước đó).

## 6. Điều gì tạo ra điểm ở mức cao nhất

`rubric.md` nói rõ: điểm tối đa của một tiêu chí đòi hỏi cả con số và cách đọc con số đó. Hai bài nộp cùng in ra "pruning ratio: 10x" không chắc đã làm lab ở cùng một mức độ. Một câu trả lời chỉ đạt mức "đủ" là dừng ở việc nêu lại con số. Một câu trả lời ở mức "tốt" giải thích được vì sao con số đó xuất hiện, ví dụ: 10 lần vì bộ lọc đặt trên cột `ts` còn Iceberg tự suy ra `ts_day` từ phép biến đổi lưu trong metadata, và một người dùng kiểu Hive quên viết điều kiện phân vùng sẽ phải đọc toàn bộ 10 file, tương đương một khoản chi phí cụ thể mỗi ngày ở một mức lưu lượng truy vấn giả định.

Checkpoint 6 và Checkpoint 7 mỗi cái đều chứa một phát hiện đo được đi ngược lại một niềm tin phổ biến: VACUUM không bắt được orphan chưa từng commit, và `expire_snapshots` của Iceberg không xoá file nào cả. Một bài nộp nhận ra và giải thích đúng một trong hai phát hiện này là bằng chứng rõ ràng cho thấy bạn thực sự đã đọc output của chính mình, không chỉ chạy notebook cho qua.

## 7. Sự cố thường gặp khi chuẩn bị nộp bài

Nếu `make run-all` báo một notebook thất bại nhưng notebook đó chạy được khi bạn mở tay trong Jupyter, khả năng cao là bạn đang chạy `make run-all` từ một thư mục làm việc khác thư mục gốc repo, hoặc dữ liệu `_lakehouse` từ một lần chạy trước đang ở trạng thái không nhất quán, hãy thử `make clean && make setup && make smoke && make data && make data-ai && make run-all` để tái tạo đúng chuỗi lệnh mà giảng viên sẽ chạy khi chấm.

Nếu bạn đang chạy trong WSL và repo nằm dưới một ổ đĩa Windows được mount qua `/mnt/*`, hãy kiểm tra lại phần Checkpoint 0 về lỗi `LocalFileSystem error: Upload aborted`, cơ chế tự chuyển `LAKEHOUSE_ROOT` sang hệ thống file Linux gốc đã được xử lý trong `scripts/lakehouse.py`, nhưng nếu bạn tự đặt `LAKEHOUSE_ROOT` trỏ ngược lại về một đường dẫn dưới `/mnt/*`, lỗi này có thể quay lại.

## 8. Ý nghĩa của checkpoint này

Checkpoint 9 khép lại vòng lặp mà cả lab theo đuổi: mọi tiêu chí kỹ thuật đều được biến thành một con số có thể kiểm tra bằng máy, nhưng điểm số cao nhất chỉ dành cho người hiểu được ý nghĩa của con số đó, không chỉ người có khả năng làm nó xuất hiện trên màn hình.
