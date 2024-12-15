FROM bitnami/spark:3.2.3

# Chuyển sang root để cài đặt
USER root

# Cài đặt Python và các dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện Python
RUN pip3 install \
    pyspark==3.2.3 \
    cassandra-driver==3.25.0 \
    pandas

# Tạo thư mục làm việc
RUN mkdir -p /opt/spark/work-dir
WORKDIR /opt/spark/work-dir

# Đảm bảo quyền truy cập
RUN chown -R 1001:1001 /opt/spark/work-dir

# Chuyển về user mặc định của Bitnami
USER 1001