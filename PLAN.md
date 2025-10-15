# Kế hoạch Triển khai Dự án: Hệ thống Gợi ý Nhạc Spotify

## Mục tiêu tổng thể

Xây dựng và triển khai một pipeline dữ liệu lớn end-to-end trên Kubernetes (sử dụng Minikube cho môi trường local) để tạo ra một hệ thống gợi ý nhạc từ bộ dữ liệu Spotify Million Playlist. Kế hoạch được chia thành các giai đoạn theo tuần, với các mục tiêu và kết quả rõ ràng.

---

## Tuần 1: Nền tảng, Môi trường và Nạp dữ liệu

**🎯 Mục tiêu của tuần:** Thiết lập toàn bộ môi trường phát triển cần thiết và đưa thành công dữ liệu thô vào Data Lake (MinIO).

1.  **Cài đặt Công cụ Local:**
    *   Cài đặt Docker, `kubectl`, `minikube`.
    *   Khởi động cụm Minikube: `minikube start`.
    *   Thiết lập môi trường Python (Conda/venv).

2.  **Tổ chức Dữ liệu:**
    *   Tải về "Spotify Million Playlist Dataset".
    *   Tạo thư mục `data/` và đặt tất cả các file `*.json` vào bên trong.
    *   Đảm bảo `data/` đã có trong file `.gitignore`.

3.  **Thiết lập Hạ tầng cơ bản trên Docker Compose:**
    *   Viết file `docker-compose.yml` để khởi chạy các dịch vụ:
        *   MinIO (Data Lake)
        *   Kafka & Zookeeper
        *   Jupyter Lab
    *   Chạy `docker-compose up` và tạo một bucket (ví dụ: `spotify-raw-data`) trên giao diện MinIO.

4.  **Viết Script Nạp Dữ liệu (Ingestion):**
    *   Hoàn thiện code trong `data_ingestion/batch_ingest.py`.
    *   Script này sẽ đọc tất cả file JSON từ thư mục `data/` và upload lên bucket `spotify-raw-data` trên MinIO.

**✅ Kết quả cuối tuần 1:** Dữ liệu thô đã nằm trên MinIO. Môi trường phát triển đã sẵn sàng.

---

## Tuần 2: Khám phá và Xử lý Dữ liệu Lõi (ETL)

**🎯 Mục tiêu của tuần:** Hiểu rõ bộ dữ liệu và xây dựng một Spark job hoàn chỉnh để chuyển đổi dữ liệu thô thành định dạng có cấu trúc, sẵn sàng cho Machine Learning.

1.  **Phân tích Khám phá Dữ liệu (EDA):**
    *   Sử dụng Jupyter Notebook (`notebooks/01_exploratory_data_analysis.ipynb`).
    *   Viết code PySpark để đọc dữ liệu mẫu từ MinIO và thực hiện các phân tích thống kê cơ bản.

2.  **Xây dựng Spark Job ETL:**
    *   Viết code chính trong `spark_jobs/etl/run_etl.py`.
    *   Job sẽ đọc toàn bộ dữ liệu JSON từ MinIO.
    *   Thực hiện các bước làm sạch, làm phẳng cấu trúc, và chọn các cột cần thiết.
    *   Ghi DataFrame đã xử lý xuống một bucket mới trên MinIO (ví dụ: `spotify-processed-data`) dưới định dạng **Parquet**.

3.  **Chạy thử và Kiểm tra:**
    *   Chạy thử Spark job trên local để đảm bảo nó kết nối và xử lý dữ liệu với MinIO một cách chính xác.

**✅ Kết quả cuối tuần 2:** Một bộ dữ liệu sạch, định dạng Parquet đã sẵn sàng trên MinIO. Có một Spark job ETL tái sử dụng được.

---

## Tuần 3: Xây dựng và Huấn luyện Mô hình Recommendation

**🎯 Mục tiêu của tuần:** Sử dụng dữ liệu đã xử lý để huấn luyện một mô hình gợi ý và đánh giá hiệu quả của nó.

1.  **Feature Engineering:**
    *   Viết logic trong Spark để chuyển đổi các ID dạng text (`track_uri`, `pid`) thành các ID dạng số nguyên (integer indices) mà các mô hình ML có thể sử dụng.

2.  **Viết Spark Job Huấn luyện Mô hình:**
    *   Hoàn thiện code trong `spark_jobs/ml/train_model.py`.
    *   Job sẽ đọc dữ liệu Parquet, thực hiện feature engineering.
    *   Sử dụng **Spark MLlib** để huấn luyện mô hình **ALS (Alternating Least Squares)**.
    *   Lưu mô hình đã được huấn luyện tốt nhất xuống MinIO.

3.  **Đánh giá Mô hình:**
    *   Chia dữ liệu thành tập train/validation.
    *   Viết code để tính toán các độ đo như `Precision@k` hoặc `nDCG@k` để đánh giá chất lượng mô hình.

**✅ Kết quả cuối tuần 3:** Một mô hình recommendation đã được huấn luyện, đánh giá và lưu lại trên MinIO.

---

## Tuần 4: Phục vụ (Serving) và Xây dựng API

**🎯 Mục tiêu của tuần:** Làm cho mô hình có thể sử dụng được bằng cách tính toán trước kết quả và cung cấp chúng qua một API.

1.  **Xây dựng Job Tính toán trước (Pre-computation):**
    *   Viết code trong `spark_jobs/serving/precompute_recommendations.py`.
    *   Job này sẽ tải mô hình đã huấn luyện, tạo ra N gợi ý hàng đầu cho mỗi playlist.
    *   Lưu kết quả vào **Redis** (thêm Redis vào `docker-compose.yml`) dưới dạng `key-value` (`playlist_id` -> `[list_of_track_ids]`).

2.  **Xây dựng API Service:**
    *   Hoàn thiện code trong `api_service/` sử dụng **FastAPI**.
    *   Tạo endpoint `GET /recommendations/{playlist_id}` để truy vấn gợi ý từ Redis.

3.  **Đóng gói API:**
    *   Viết `api_service/Dockerfile` để container hóa ứng dụng FastAPI.
    *   Build và chạy thử container trên local để kiểm tra.

**✅ Kết quả cuối tuần 4:** Một API service chạy trong Docker có thể trả về gợi ý nhạc cho một playlist bất kỳ.

---

## Tuần 5: Hoàn thiện, Triển khai và Trình bày

**🎯 Mục tiêu của tuần:** Đưa toàn bộ hệ thống lên Kubernetes (Minikube) và hoàn thiện tài liệu để sẵn sàng trình bày.

1.  **Viết Manifests Kubernetes:**
    *   Hoàn thiện các file `.yaml` trong thư mục `kubernetes/`.
    *   `core-infra`: Deploy MinIO, Redis.
    *   `apps`: Viết `Deployment` và `Service` cho API.
    *   `jobs`: Viết file định nghĩa `Job` để chạy các tác vụ Spark trên K8s.

2.  **Triển khai lên Minikube:**
    *   Sử dụng `kubectl apply -f <folder>` để triển khai toàn bộ ứng dụng.
    *   Kiểm tra trạng thái các Pods, Services và gọi thử API.

3.  **Tự động hóa (Điểm cộng):**
    *   Nếu có thời gian, viết một DAG đơn giản trong `dags/` cho Airflow/Dagster để tự động hóa chuỗi: ETL -> Training -> Pre-computation.

4.  **Hoàn thiện Tài liệu:**
    *   Cập nhật file `README.md` với mô tả kiến trúc, hướng dẫn cài đặt chi tiết và kết quả dự án.

**✅ Kết quả cuối tuần 5:** Một dự án Big Data hoàn chỉnh, có thể demo chạy end-to-end trên một cụm Kubernetes. Tài liệu rõ ràng, sẵn sàng cho việc trình bày.