import sys
import os
import glob


node_cnt=40
client_node_cnt=8
shard_cnt=4
cross_num=2

def process_node_log(content):
    """处理node端日志，提取最后出现的关键指标"""
    results = {
        'tput': None,
        'valid_tput': None,
        'cross_shard_tput': None,
        'no_cross_shard_tput': None,
        'node_latency': None
    }
    
    # 按行处理日志
    for line in content.split('\n'):
        # 提取tput
        if line.startswith('tput         ='):
            try:
                results['tput'] = float(line.split('=')[1].split('\t')[0].strip())
            except:
                pass
        # 提取valid_tput
        elif line.startswith('valid_tput         ='):
            try:
                results['valid_tput'] = float(line.split('=')[1].split('\t')[0].strip())
            except:
                pass
        # 提取cross_shard_tput，并在读取时就除以cross_num
        elif line.startswith('cross_shard_tput         ='):
            try:
                results['cross_shard_tput'] = float(line.split('=')[1].split('\t')[0].strip()) / cross_num
            except:
                pass
        # 提取no_cross_shard_tput
        elif line.startswith('no_cross_shard_tput         ='):
            try:
                results['no_cross_shard_tput'] = float(line.split('=')[1].split('\t')[0].strip())
            except:
                pass
        # 提取NodeLatency
        elif line.startswith('NodeLatency='):
            try:
                results['node_latency'] = float(line.split('=')[1].strip())
            except:
                pass
    
    return results

def process_client_log(content):
    """处理client端日志，提取Latency"""
    result = {
        'latency': None
    }
    
    # 按行处理日志
    for line in content.split('\n'):
        if line.startswith('Latency='):
            try:
                result['latency'] = float(line.split('=')[1].strip())
            except:
                pass
    
    return result

def calculate_averages(node_results, file):
    """计算所有节点的平均值"""
    metrics = ['tput', 'valid_tput', 'cross_shard_tput', 'no_cross_shard_tput', 'node_latency']
    averages = {}
    
    for metric in metrics:
        values = [r[metric] for r in node_results if r[metric] is not None]
        if values:
            averages[metric] = sum(values) / len(values)
        else:
            averages[metric] = 0
            
        # 打印每个节点的值，用于调试
        print_and_save(f"\n{metric} 各节点值:", file)
        for i, r in enumerate(node_results):
            print_and_save(f"Node {i}: {r[metric]}", file)
    
    # 计算实际tps
    averages['actual_tps'] = averages['cross_shard_tput'] + averages['no_cross_shard_tput']
    
    return averages

def calculate_client_averages(client_results, file):
    """计算client端的平均延迟"""
    latency_values = [r['latency'] for r in client_results if r['latency'] is not None]
    
    print_and_save("\nClient端各节点Latency值:", file)
    for i, r in enumerate(client_results):
        if r['latency'] is not None:
            print_and_save(f"Client {i}: {r['latency']:.6f}", file)
    
    if latency_values:
        avg_latency = sum(latency_values) / len(latency_values)
        return avg_latency
    return 0

# def calculate_system_tps(node_results, node_cnt, shard_cnt):
#     """计算系统TPS，返回总TPS和分片详情"""
#     nodes_per_shard = node_cnt // shard_cnt
#     shard_tps_list = []
#     shard_details = []
    
#     # 遍历每个分片
#     for shard_idx in range(shard_cnt):
#         shard_start = shard_idx * nodes_per_shard
#         shard_end = shard_start + nodes_per_shard
#         shard_tps = []
        
#         # 记录分片信息
#         shard_details.append(f"\n分片 {shard_idx} (Node {shard_start}-{shard_end-1}) 的节点TPS值:")
        
#         # 计算该分片内每个节点的TPS
#         for node_idx in range(shard_start, shard_end):
#             if node_idx < len(node_results):
#                 node = node_results[node_idx]
#                 if node['cross_shard_tput'] is not None and node['no_cross_shard_tput'] is not None:
#                     node_tps = node['cross_shard_tput'] + node['no_cross_shard_tput']
#                     shard_tps.append(node_tps)
#                     shard_details.append(f"Node {node_idx}: {node_tps:.2f}")
        
#         # 计算该分片的平均TPS
#         if shard_tps:
#             shard_avg = sum(shard_tps) / len(shard_tps)
#             shard_tps_list.append(shard_avg)
#             shard_details.append(f"分片 {shard_idx} 平均TPS: {shard_avg:.2f}")
    
#     # 计算系统总TPS
#     system_tps = sum(shard_tps_list)
#     return system_tps, shard_details

def calculate_system_tps(node_results, node_cnt, shard_cnt):
    """计算系统TPS，返回总TPS、分片详情和跨片率信息"""
    nodes_per_shard = node_cnt // shard_cnt
    shard_tps_list = []
    shard_details = []
    shard_cross_rates = []  # 新增：存储每个分片的跨片率
    
    # 遍历每个分片
    for shard_idx in range(shard_cnt):
        shard_start = shard_idx * nodes_per_shard
        shard_end = shard_start + nodes_per_shard
        shard_tps = []
        shard_cross_rates_per_node = []  # 新增：存储该分片内每个节点的跨片率
        
        # 记录分片信息
        shard_details.append(f"\n分片 {shard_idx} (Node {shard_start}-{shard_end-1}) 的节点信息:")
        
        # 计算该分片内每个节点的TPS和跨片率
        for node_idx in range(shard_start, shard_end):
            if node_idx < len(node_results):
                node = node_results[node_idx]
                if node['cross_shard_tput'] is not None and node['no_cross_shard_tput'] is not None:
                    node_tps = node['cross_shard_tput'] + node['no_cross_shard_tput']
                    # 计算节点跨片率
                    cross_rate = (node['cross_shard_tput'] / node_tps * 100) if node_tps > 0 else 0
                    shard_tps.append(node_tps)
                    shard_cross_rates_per_node.append(cross_rate)
                    shard_details.append(f"Node {node_idx}: TPS={node_tps:.2f}, 跨片率={cross_rate:.2f}%")
        
        # 计算该分片的平均TPS和平均跨片率
        if shard_tps:
            shard_avg = sum(shard_tps) / len(shard_tps)
            shard_avg_cross_rate = sum(shard_cross_rates_per_node) / len(shard_cross_rates_per_node)
            shard_tps_list.append(shard_avg)
            shard_cross_rates.append(shard_avg_cross_rate)
            shard_details.append(f"分片 {shard_idx} 平均TPS: {shard_avg:.2f}, 平均跨片率: {shard_avg_cross_rate:.2f}%")
    
    # 计算系统总TPS和整体平均跨片率
    system_tps = sum(shard_tps_list)
    system_avg_cross_rate = sum(shard_cross_rates) / len(shard_cross_rates) if shard_cross_rates else 0
    
    return system_tps, shard_details, system_avg_cross_rate

def read_file_content(filename):
    """使用多种编码尝试读取文件"""
    encodings = ['utf-8', 'latin1', 'gbk', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(filename, encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    
    # 如果所有编码都失败，使用二进制模式读取
    try:
        with open(filename, 'rb') as f:
            return f.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"警告: 无法读取文件 {filename}: {str(e)}")
        return ""

# 在处理node端日志的部分修改为：
node_results = []
for i in range(node_cnt):
    filename = f"results/{i}.out"
    if os.path.exists(filename):
        print(f"正在读取 {filename}")
        content = read_file_content(filename)
        node_results.append(process_node_log(content))
    else:
        print(f"警告: {filename} 不存在")

# 在处理client端日志的部分修改为：
client_results = []
for i in range(node_cnt, node_cnt + client_node_cnt):
    filename = f"results/{i}.out"
    if os.path.exists(filename):
        print(f"正在读取 {filename}")
        content = read_file_content(filename)
        client_results.append(process_client_log(content))
    else:
        print(f"警告: {filename} 不存在")


# 添加结果处理和输出
# print("\n处理结果统计：")
# print(f"成功处理的node端日志数量：{len(node_results)}")
# print(f"成功处理的client端日志数量：{len(client_results)}")

# print("\nNode端性能指标平均值：")
# averages = calculate_averages(node_results)
# print(f"平均 tput: {averages['tput']:.2f}")
# print(f"平均 valid_tput: {averages['valid_tput']:.2f}")
# print(f"平均 cross_shard_tput: {averages['cross_shard_tput']:.2f}")
# print(f"平均 no_cross_shard_tput: {averages['no_cross_shard_tput']:.2f}")
# print(f"平均 NodeLatency: {averages['node_latency']:.6f}")
# print(f"平均 actual_tps: {averages['actual_tps']:.2f}")

# print("\nClient端性能指标：")
# avg_latency = calculate_client_averages(client_results)
# print(f"平均 Latency: {avg_latency:.6f}")

# # 计算并打印系统TPS
# system_tps = calculate_system_tps(node_results, node_cnt, shard_cnt)
# print(f"\n系统 TPS: {system_tps:.2f}")


# 获取运行参数，默认为"exp"
result_id = "exp"
if len(sys.argv) > 1:
    result_id = sys.argv[1]

# 创建输出文件
output_file = f"results/result_{result_id}.out"

# 将print输出同时写入文件的函数
def print_and_save(content, file):
    print(content)
    file.write(content + "\n")

# 打开输出文件
with open(output_file, 'w') as f:
    # 第一步：计算系统TPS并立即写入第一行
    system_tps, shard_details, system_avg_cross_rate = calculate_system_tps(node_results, node_cnt, shard_cnt)
    f.write(f"系统 TPS: {system_tps:.2f}\n")
    f.write(f"系统平均跨片率: {system_avg_cross_rate:.2f}%\n\n")
    
    # 第二步：写入分片详情
    for line in shard_details:
        f.write(line + '\n')
    
    # 第三步：Node端性能指标
    f.write("\nNode端性能指标平均值：\n")
    averages = calculate_averages(node_results, f)
    f.write(f"平均 tput: {averages['tput']:.2f}\n")
    f.write(f"平均 valid_tput: {averages['valid_tput']:.2f}\n")
    f.write(f"平均 cross_shard_tput: {averages['cross_shard_tput']:.2f}\n")
    f.write(f"平均 no_cross_shard_tput: {averages['no_cross_shard_tput']:.2f}\n")
    f.write(f"平均 actual_tps: {averages['actual_tps']:.2f}\n")
    f.write(f"平均 NodeLatency: {averages['node_latency']:.6f}\n")

    # 第四步：Client端性能指标
    f.write("\nClient端性能指标：\n")
    avg_latency = calculate_client_averages(client_results, f)
    f.write(f"平均 Latency: {avg_latency:.6f}\n")

    # 计算系统TPS和跨片率
    system_tps, shard_details, system_avg_cross_rate = calculate_system_tps(node_results, node_cnt, shard_cnt)
    f.write(f"系统 TPS: {system_tps:.2f}\n")
    f.write(f"系统平均跨片率: {system_avg_cross_rate:.2f}%\n\n")

print(f"\n结果已保存至: {output_file}")