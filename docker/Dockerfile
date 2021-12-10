FROM python:3.8

# 设置时区
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime; echo "Asia/Shanghai" > /etc/timezone; echo "export TZ=Asia/Shanghai" >> /etc/profile;
ENV TZ="Asia/Shanghai"

COPY sources.list /etc/apt/sources.list

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 871920D1991BC93C && \
    apt update && \
    apt install -y --allow-unauthenticated \
    vim \
    && apt clean

RUN pip install -i https://mirrors.aliyun.com/pypi/simple --upgrade pip &&\
    pip install -i https://mirrors.aliyun.com/pypi/simple --no-cache-dir graper;