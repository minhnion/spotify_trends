#!/bin/bash

# Tạo thư mục jars nếu chưa có
mkdir -p jars

# 1. Xóa sạch file cũ để đảm bảo không còn file lỗi
echo "🧹 Đang dọn dẹp thư mục jars/..."
rm -f jars/*.jar

echo "⬇ Bắt đầu tải các thư viện về thư mục 'jars/'..."
echo "---------------------------------------------------"

# ==========================================
# 1. NHÓM MONGODB (Giữ nguyên)
# ==========================================
echo "[1/4] Downloading MongoDB Ecosystem..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.2/mongodb-driver-sync-4.11.2.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.2/mongodb-driver-core-4.11.2.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/bson/4.11.2/bson-4.11.2.jar

# ==========================================
# 2. NHÓM AWS S3 (Giữ nguyên Hadoop 3.3.4)
# ==========================================
echo "[2/4] Downloading AWS S3 Support..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
# AWS SDK Bundle này tương thích tốt với Hadoop 3.3.4
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ==========================================
# 3. NHÓM ICEBERG (HẠ CẤP VỀ 1.4.3)
# Sửa đổi quan trọng: Dùng 1.4.3 để tương thích Hadoop 3.3.4
# Bỏ iceberg-aws-bundle vì runtime jar đã đủ dùng cho Spark
# ==========================================
echo "[3/4] Downloading Iceberg (Downgraded to 1.4.3)..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# ==========================================
# 4. NHÓM KAFKA & POSTGRES (Giữ nguyên)
# ==========================================
echo "[4/4] Downloading Kafka & Postgres..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
# wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

echo "---------------------------------------------------"
echo "✅ HOÀN TẤT! Hãy chạy lại job Spark của bạn."
ls -lh jars/