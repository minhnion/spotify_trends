#!/bin/bash

# Tạo thư mục jars nếu chưa có
mkdir -p jars

# 1. Xóa sạch file cũ để tránh lỗi file corrupt hoặc xung đột version
echo "Đang dọn dẹp thư mục jars/..."
rm -f jars/*.jar

echo "⬇Bắt đầu tải các thư viện về thư mục 'jars/'..."
echo "---------------------------------------------------"

# ==========================================
# 1. NHÓM MONGODB (Version: 10.3.0 + Driver 4.11.2)
# ==========================================
echo "[1/5] Downloading MongoDB Ecosystem..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.2/mongodb-driver-sync-4.11.2.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.2/mongodb-driver-core-4.11.2.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/mongodb/bson/4.11.2/bson-4.11.2.jar

# ==========================================
# 2. NHÓM AWS S3 (Version: Hadoop 3.3.4 + SDK 1.12.262)
# Lưu ý: SDK Bundle rất nặng (~268MB), hãy kiên nhẫn
# ==========================================
echo "[2/5] Downloading AWS S3 Support (Heavy file)..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ==========================================
# 3. NHÓM ICEBERG (Version: 1.5.2 for Spark 3.5)
# ==========================================
echo "[3/5] Downloading Iceberg..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar

# ==========================================
# 4. NHÓM KAFKA (Version: Spark 3.5.1)
# Mình nâng nhẹ spark-token và spark-sql lên 3.5.1 để khớp với kafka-clients 3.5.1 của bạn
# ==========================================
echo "[4/5] Downloading Kafka Streaming..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ==========================================
# 5. NHÓM POSTGRESQL (Version: 42.6.0)
# ==========================================
echo "[5/5] Downloading Postgres JDBC..."
wget -q --show-progress -P jars/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

echo "---------------------------------------------------"
echo "HOÀN TẤT! Kiểm tra danh sách file:"
ls -lh jars/