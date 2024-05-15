FROM ubuntu:22.04
WORKDIR /app

# 使用 & 来连接命令可能不会按预期工作，因为它们是在同一个RUN指令中执行的。
# 建议将每个命令放在单独的RUN指令中，以确保正确执行和层的缓存。

# 安装构建工具和依赖项
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y cmake && \
    apt-get install -y libboost-all-dev && \
    apt-get install -y wget && \
    apt-get install -y unzip && \
    apt-get install -y htop && \
    apt-get install -y git && \
    apt-get install default-jdk && \
    apt-get install bc && \
    apt-get install tmux
# 安装Python 3和pip
RUN apt update && \
    apt install -y python3 python3-pip

# 安装GTK+接口库，用于图形界面应用程序（如果需要）
RUN apt install -y libgtk-3-dev

RUN git clone https://github.com/kpu/kenlm.git \
&& cd kenlm \
&& pip install .

COPY requirements.txt .

RUN pip install -r requirements.txt