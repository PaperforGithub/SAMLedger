import sys
import os
import subprocess
import time
import shutil
import re
def generate_workload(distribution_type: str, theta: float = None, hotness: float = 0.2, read_hot: int = 80, file: str = None):
    """根据指定的分布类型生成工作负载
    
    Args:
        distribution_type: 分布类型 ('zipf', 'hot' 或 'modified')
        theta: Zipf分布参数
        hotness: 热点区域占比
        read_hot: 访问热点区域的概率
        file: modified类型时使用的预设文件名
    """
    if distribution_type == 'modified':
        print(f"使用预设工作负载文件: {file}...")
        input_file = os.path.join('./input', file)
        shutil.copy2(input_file, './shuffled_transactions.txt')
    elif distribution_type == 'zipf':
        print(f"生成Zipf分布工作负载 (theta={theta})...")
        subprocess.run(['python3', './scripts/genz_zipf.py', '--theta', str(theta)])
    else:  # hot
        print(f"生成热点分布工作负载 (hotness={hotness}, read_hot={read_hot})...")
        subprocess.run(['python3', './scripts/genz_hot.py', '--hotness', str(hotness), '--read_hot', str(read_hot)])


def modify_config(file_path, parameter, value):
    """修改config.h中的参数值"""
    with open(file_path, 'r') as file:
        content = file.read()
    
    # 修改正则表达式以匹配数值、布尔值和其他可能的值
    pattern = f'#define\s+{parameter}\s+(true|false|[0-9.]+)'
    replacement = f'#define {parameter} {value}'
    
    new_content = re.sub(pattern, replacement, content)
    
    with open(file_path, 'w') as file:
        file.write(new_content)

def modify_prob_array(file_path, array_name, prob_values):
    """修改global.cpp中的概率数组"""
    with open(file_path, 'r') as file:
        content = file.read()
    
    array_str = f"double {array_name}[NODE_CNT/SHARD_SIZE] = {{\n\t"
    array_str += ",\n\t".join([f"{val}" for val in prob_values])
    array_str += ",\n};"
    
    pattern = f"double {array_name}\[NODE_CNT/SHARD_SIZE\] = {{[^}}]*}};"
    new_content = re.sub(pattern, array_str, content, flags=re.MULTILINE)
    
    with open(file_path, 'w') as file:
        file.write(new_content)

def run_experiment(log_path):
    """运行单次实验"""
    subprocess.run(['python3', './scripts/StopSystem.py'])
    subprocess.run(['make', 'clean'])
    subprocess.run(['make', '-j8'])
    subprocess.run(['python3', './scripts/scp_binaries.py'])
    subprocess.run(['python3', './scripts/RunSystem.py'])
    time.sleep(60)
    subprocess.run(['python3', './scripts/StopSystem.py'])
    time.sleep(10)
    subprocess.run(['python3', './scripts/scp_results.py'])

def main():
    if len(sys.argv) != 2:
        print("请输入日志路径")
        sys.exit(1)
    
    log_path = sys.argv[1]
    config_file = "./config.h"
    global_file = "./system/global.cpp"
    result_base_path = os.path.expanduser(f"~/expr-result-0409/{log_path}")
    
    # 创建结果目录
    os.makedirs(result_base_path, exist_ok=True)
    
    # 定义两种概率分布
    prob_distributions = {
        "equal": [25.0, 25.0, 25.0, 25.0]
        # ,
        # "unequal": [80.0, 10.0, 5.0, 5.0]
    }

    # 定义四种系统配置组合

    system_configs = [
        {
            "name": "Sharper",
            "config": {
            }
        },
        {
            "name": "ShardScheduler,
            "config": {
            }
        },
        {
            "name": "TxAllo",
            "config": {
            }
        },
        {
            "name": "SAMLedger-0",
            "config": {
            }
        },
        {
            "name": "SAMLedger-1",
            "config": {
            }
        },
        {
            "name": "SAMLedger",
            "config": {
            }
        }

    ]
    # 定义参数范围
    cross_shard_percentages = [10]
    distribution_configs = [
        # {'type': 'zipf', 'params': {'theta': 0.0}},
        {'type': 'zipf', 'params': {'theta': 0.99}},
        {'type': 'hot', 'params': {'hotness': 0.01, 'read_hot': 90}},
        {'type': 'modified', 'params': {'file': 'shuffled_transactions_modified_30to40.txt'}}
        # {'type': 'hot', 'params': {'hotness': 0.001, 'read_hot': 90}},
    ]
    
    # 五重循环遍历所有参数组合
    for sys_config in system_configs:
        # 修改系统配置
        for param, value in sys_config["config"].items():
            modify_config(config_file, param, value)
            
        # for shard_prob_type in ["equal", "unequal"]:
        #     for no_cross_prob_type in ["equal", "unequal"]:
        for shard_prob_type in ["equal"]:
            for no_cross_prob_type in ["equal"]:
                # 修改概率数组
                modify_prob_array(global_file, "g_random_choose_shard_prob", 
                                prob_distributions[shard_prob_type])
                modify_prob_array(global_file, "g_random_choose_no_cross_shard_prob", 
                                prob_distributions[no_cross_prob_type])
                
                for cross_percent in cross_shard_percentages:
                    for dist_config in distribution_configs:
                        # 修改参数
                        modify_config(config_file, "CROSS_SHARD_PRECENTAGE", cross_percent)

                        # 根据分布类型设置不同的参数和目录名
                        if dist_config['type'] == 'zipf':
                            modify_config(config_file, "ZIPF_THETA", dist_config['params']['theta'])
                            theta_str = str(dist_config['params']['theta'])
                            dist_str = f"zipf{theta_str}"
                        elif dist_config['type'] == 'hot':
                            modify_config(config_file, "ZIPF_THETA", 0.0)  # 重置theta
                            hotness = dist_config['params']['hotness']
                            read_hot = dist_config['params']['read_hot']
                            dist_str = f"hot{hotness}_{read_hot}"
                        else:  # modified
                            modify_config(config_file, "ZIPF_THETA", 0.0)  # 重置theta
                            dist_str = dist_config['params']['file'].replace('shuffled_transactions_', '').replace('.txt', '')
                        
                        # 创建实验结果目录
                        experiment_dir = (f"sys_{sys_config['name']}_"
                                        f"shard_{shard_prob_type}_"
                                        f"nocross_{no_cross_prob_type}_"
                                        f"cross{cross_percent}_"
                                        f"dist_{dist_str}")
                        result_path = os.path.join(result_base_path, experiment_dir)
                        os.makedirs(result_path, exist_ok=True)
                        
                        # 生成工作负载并运行实验
                        print(f"\n开始实验: "
                              f"System={sys_config['name']}, "
                              f"Shard_Prob={shard_prob_type}, "
                              f"NoCross_Prob={no_cross_prob_type}, "
                              f"CROSS_SHARD_PRECENTAGE={cross_percent}, "
                              f"Distribution={dist_str}")
                        
                        generate_workload(distribution_type=dist_config['type'], 
                                        **dist_config['params'])
                        run_experiment(log_path)
                        
                        # 复制结果
                        if os.path.exists("./results"):
                            shutil.copytree("./results", result_path, dirs_exist_ok=True)

                        # 保存config.h到结果目录
                        shutil.copy2(config_file, os.path.join(result_path, "config.h"))

if __name__ == "__main__":
    main()