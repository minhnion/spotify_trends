# Dự án Big Data: Hệ thống Gợi ý Nhạc Spotify trên Kubernetes

Tài liệu này hướng dẫn chi tiết các bước để triển khai và vận hành toàn bộ pipeline dữ liệu lớn trên một cụm Kubernetes (sử dụng Minikube cho môi trường local).

## 📋 Mục lục
1.  [Tổng quan Kiến trúc](#-tổng-quan-kiến-trúc)
2.  [Yêu cầu Chuẩn bị](#-yêu-cầu-chuẩn-bị)
3.  [Bước 1: Khởi tạo Môi trường Kubernetes](#-bước-1-khởi-tạo-môi-trường-kubernetes)
4.  [Bước 2: Triển khai Hạ tầng Cốt lõi](#-bước-2-triển-khai-hạ-tầng-cốt-lõi)
5.  [Bước 3: Build và Đóng gói Ứng dụng](#-bước-3-build-và-đóng-gói-ứng-dụng)
6.  [Bước 4: Triển khai API Service](#-bước-4-triển-khai-api-service)
7.  [Bước 5: Thực thi Pipeline Dữ liệu Lớn](#-bước-5-thực-thi-pipeline-dữ-liệu-lớn)
8.  [Bước 6: Truy cập và Demo Hệ thống](#-bước-6-truy-cập-và-demo-hệ-thống)
9.  [Dọn dẹp](#-dọn-dẹp)

---

## 🏛️ Tổng quan Kiến trúc

Hệ thống được thiết kế theo kiến trúc Big Data hiện đại, bao gồm các thành phần chính được dàn dựng hoàn toàn trên Kubernetes:
*   **Data Lake:** MinIO, lưu trữ dữ liệu thô và đã qua xử lý.
*   **Processing Engine:** Apache Spark, chạy các job ETL, ML training dưới dạng các Pods chuyên dụng.
*   **Serving Cache:** Redis, lưu trữ các kết quả gợi ý đã được tính toán trước để truy vấn tức thì.
*   **Application Layer:** FastAPI Service, cung cấp API để truy vấn gợi ý.

---

## 🛠️ Yêu cầu Chuẩn bị

Trước khi bắt đầu, hãy đảm bảo bạn đã cài đặt các công cụ sau:
*   [Docker Desktop](https://www.docker.com/products/docker-desktop/)
*   [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
*   [Minikube](https://minikube.sigs.k8s.io/docs/start/)
*   Git

---

## 🚀 Bước 1: Khởi tạo Môi trường Kubernetes

1.  **Khởi động cụm Minikube:**
    Mở terminal và chạy lệnh sau. Quá trình này có thể mất vài phút.
    ```bash
    minikube start --cpus 4 --memory 4096 # Đề xuất cấp ít nhất 4 CPU và 4GB RAM
    ```

2.  **Tạo Namespace:**
    Chúng ta sẽ gom nhóm tất cả tài nguyên của dự án vào một namespace tên là `spotify`.
    ```bash
    kubectl apply -f kubernetes/core-infra/00-namespace.yaml
    ```

3.  **Tạo Secret cho MinIO:**
    Lưu trữ thông tin đăng nhập MinIO một cách an toàn.
    ```bash
    kubectl create secret generic minio-secret \
      --from-literal=MINIO_ROOT_USER=minioadmin \
      --from-literal=MINIO_ROOT_PASSWORD=minioadmin \
      -n spotify
    ```

    Kiểm tra 
    ```bash
    kubectl get secret minio-secret -n spotify
    ```

4.  **Tạo Secret cho MongoDB:**
    Lưu trữ thông tin kết nối MonggoDB một cách an toàn.
    ```bash
    kubectl create secret generic mongodb-secret -n spotify \
        --from-literal=MONGO_URI='mongodb+srv://nguyenhoangviethung_db_user:QPB5DBekdXmI68rn@cluster0.fuxyyc0.mongodb.net' \
        --from-literal=MONGO_DATABASE='spotify_trends'
    ```

    Kiểm tra
    ```bash
    kubectl get secret mongodb-secret -n spotify
    ```
---

## 🏗️ Bước 2: Triển khai Hạ tầng Cốt lõi

Triển khai MinIO (Data Lake) và Redis (Cache) lên cụm Kubernetes.

1.  **Triển khai MinIO:**
    File cấu hình này sẽ tạo một vùng lưu trữ bền vững (PVC), một Deployment để quản lý Pod MinIO, và một Service để các thành phần khác có thể giao tiếp với nó.
    ```bash
    kubectl apply -f kubernetes/core-infra/minio.yaml
    ```

2.  **Triển khai Redis:**
    ```bash
    kubectl apply -f kubernetes/core-infra/redis.yaml
    ```

3.  **Kiểm tra trạng thái:**
    Đợi một lát và kiểm tra xem các Pod đã chuyển sang trạng thái `Running` hay chưa.
    ```bash
    kubectl get all -n spotify
    ```
    Bạn sẽ thấy các deployment, service, và pod của minio và redis.

---

## 📦 Bước 3: Build và Đóng gói Ứng dụng

Kubernetes cần các image Docker để chạy ứng dụng. Chúng ta sẽ build các image này và đưa chúng vào môi trường Docker nội bộ của Minikube.

1.  **Trỏ Docker CLI vào Minikube:**
    Chạy lệnh này để các lệnh `docker` tiếp theo sẽ làm việc với Docker daemon bên trong Minikube.
    ```bash
    eval $(minikube -p minikube docker-env)
    ```
    > **Lưu ý:** Bạn cần chạy lại lệnh này nếu mở một cửa sổ terminal mới.

2.  **Build image cho API Service:**
    ```bash
    docker build -t spotify-api:latest ./api_service
    ```

3.  **Build image cho các Spark Job:**
    ```bash
    docker build -t spark-jobs:latest -f spark_jobs/Dockerfile .
    ```

---

## 🚢 Bước 4: Triển khai API Service

Triển khai ứng dụng FastAPI đã được đóng gói ở bước trên.

1.  **Áp dụng file Deployment và Service:**
    ```bash
    kubectl apply -f kubernetes/apps/api-service-deployment.yaml
    kubectl apply -f kubernetes/apps/api-service-service.yaml
    ```
2.  **Kiểm tra lại:**
    ```bash
    kubectl get deployment,pods,svc -n spotify
    ```
    Bạn sẽ thấy deployment và các pod của `api-deployment` đã được tạo ra.

---

## ✨ Bước 5: Thực thi Pipeline Dữ liệu Lớn

Bây giờ hạ tầng đã sẵn sàng, chúng ta sẽ nạp dữ liệu và chạy các job xử lý.

1.  **Nạp dữ liệu thô vào MinIO trên Kubernetes:**
    *   **Mở một terminal mới** và chạy lệnh port-forward để có thể truy cập MinIO từ máy local:
        ```bash
        kubectl port-forward service/minio-service 9000:9000 -n spotify
        ```
    *   **Mở một terminal khác**, kích hoạt môi trường ảo (`source venv/bin/activate`) và chạy script ingestion:
        ```bash
        python data_ingestion/batch_ingest.py
        ```
    *   Sau khi script chạy xong, bạn có thể đóng terminal port-forward.

2.  **Chạy các Spark Job trên Kubernetes:**
    Các script này sẽ submit job lên K8s, yêu cầu K8s tự tạo các Pod Spark để thực thi.
    *   Cấp quyền thực thi cho các script:
        ```bash
        chmod +x scripts/*_on_k8s.sh
        ```
    *   Chạy các job theo đúng thứ tự:
        ```bash
        # 1. Chạy ETL để xử lý dữ liệu
        ./scripts/run_etl_on_k8s.sh

        # 2. Ghi dữ liệu vào mongodb
        ./scripts/run_db_population_on_k8s.sh

        # 3. Huấn luyện model
        ./scripts/run_training_on_k8s.sh

        # 4. Tính toán trước và lưu kết quả vào Redis
        ./scripts/run_precomputation_on_k8s.sh
        ```
    *   Để theo dõi tiến trình, bạn có thể mở một terminal khác và chạy: `kubectl get pods -n spotify --watch`.

---

## 🎯 Bước 6: Truy cập và Demo Hệ thống

1.  **Truy cập API:**
    Mở giao diện Swagger UI trong trình duyệt để tương tác và demo.
    ```bash
    minikube service api-service -n spotify
    ```
    Nhập một `playlist_id` (ví dụ: 123, 456) để nhận kết quả gợi ý.

2.  **Truy cập MinIO UI:**
    Kiểm tra dữ liệu đã được xử lý và model đã được lưu.
    ```bash
    minikube service minio-service -n spotify
    ```

3.  **Demo các tính năng của Kubernetes:**
    *   **Tự phục hồi (Self-Healing):**
        ```bash
        # Cửa sổ 1: Theo dõi
        kubectl get pods -n spotify --watch
        # Cửa sổ 2: Xóa một pod API
        kubectl delete pod <tên-pod-api> -n spotify
        ```
    *   **Mở rộng quy mô (Scaling):**
        ```bash
        # Xem số lượng pod tăng lên
        kubectl scale deployment api-deployment --replicas=4 -n spotify
        ```

---

## 🧹 Dọn dẹp

Để dừng và xóa toàn bộ tài nguyên đã tạo, chạy các lệnh sau:
```bash
minikube stop
minikube delete --all