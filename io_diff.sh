
# 读取 old_nvme.log 中的磁盘读写量
old_nvme_disk1_read=$(grep "nvme0n1 总读取量" ./old_nvme.log | awk '{print $4}' | sed 's/MB//')
old_nvme_disk1_write=$(grep "nvme0n1 总写入量" ./old_nvme.log | awk '{print $4}' | sed 's/MB//')
old_nvme_disk2_read=$(grep "nvme1n1 总读取量" ./old_nvme.log | awk '{print $4}' | sed 's/MB//')
old_nvme_disk2_write=$(grep "nvme1n1 总写入量" ./old_nvme.log | awk '{print $4}' | sed 's/MB//')

# 读取 new_nvme.log 中的磁盘读写量
new_nvme_disk1_read=$(grep "nvme0n1 总读取量" ./new_nvme.log | awk '{print $4}' | sed 's/MB//')
new_nvme_disk1_write=$(grep "nvme0n1 总写入量" ./new_nvme.log | awk '{print $4}' | sed 's/MB//')
new_nvme_disk2_read=$(grep "nvme1n1 总读取量" ./new_nvme.log | awk '{print $4}' | sed 's/MB//')
new_nvme_disk2_write=$(grep "nvme1n1 总写入量" ./new_nvme.log | awk '{print $4}' | sed 's/MB//')

# 计算差值并换算成GB
disk1_read_diff=$(echo "scale=2; ($new_nvme_disk1_read - $old_nvme_disk1_read) / 1024" | bc)
disk1_write_diff=$(echo "scale=2; ($new_nvme_disk1_write - $old_nvme_disk1_write) / 1024" | bc)
disk2_read_diff=$(echo "scale=2; ($new_nvme_disk2_read - $old_nvme_disk2_read) / 1024" | bc)
disk2_write_diff=$(echo "scale=2; ($new_nvme_disk2_write - $old_nvme_disk2_write) / 1024" | bc)

# 输出差值
echo "Disk nvme0n1 差值: $disk1_read_diff GB (读取), $disk1_write_diff GB (写入)"
echo "Disk nvme1n1 差值: $disk2_read_diff GB (读取), $disk2_write_diff GB (写入)"