# Reflection

Trong "Top 5 Lakehouse Anti-Patterns", nhóm em nghĩ dữ liệu của mình dễ vướng nhất vào small files kết hợp thiếu maintenance job định kỳ.

Bronze `llm_calls_raw` ở NB4 được ghi kiểu ingest liên tục, giống mô hình streaming trigger ngắn mà NB2 và NB6 mô phỏng bằng 200 lần append nhỏ. Nếu triển khai thật ở tần suất tương tự mà không có cron compact, số file tăng gần tuyến tính theo số lần ghi, kéo theo chi phí GET và thời gian lập kế hoạch truy vấn tăng phi tuyến. NB6 đo được: compaction giảm số file ít nhất 10 lần, Z-order giúp bỏ qua ít nhất 50% file khi truy vấn điểm.

Rủi ro lớn hơn là orphan file. NB6 cho thấy VACUUM của deltalake không bắt được file do writer crash để lại dù đã 30 ngày tuổi, vì file chưa từng commit vào log thì log không biết nó tồn tại. Nếu pipeline ingest LLM log chạy 24/7 mà chỉ dựa vào VACUUM, không có job quét orphan riêng, storage sẽ âm thầm phình ra mà không ai nhận ra, đúng kiểu lỗi "vô hình" slide cảnh báo.
