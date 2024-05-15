#!/bin/bash

# 定义每个扇区的大小（以字节为单位）
sector_size=512

# 定义要统计的 nvme 磁盘名称
nvme_disks="nvme0n1 nvme1n1"

# 循环遍历每个磁盘
for disk_name in $nvme_disks
do
    # 读取 /proc/diskstats 文件并查找当前磁盘名称的行
    disk_stats=$(grep " $disk_name " /proc/diskstats)

    # 解析行中的读写数据字段
    read_sectors=$(echo $disk_stats | awk '{print $6}')
    write_sectors=$(echo $disk_stats | awk '{print $10}')

    # 将扇区数转换为 MB
    read_MB=$(echo "scale=2; $read_sectors * $sector_size / 1024 / 1024" | bc)
    write_MB=$(echo "scale=2; $write_sectors * $sector_size / 1024 / 1024" | bc)

    echo "Disk $disk_name 总读取量: ${read_MB}MB"
    echo "Disk $disk_name 总写入量: ${write_MB}MB"
done
