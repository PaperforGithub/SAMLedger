import random
from typing import List, Tuple

class HotSpotGenerator:
    def __init__(self, max_account: int, hotness: float, read_hot: int):
        """初始化热点生成器
        
        Args:
            max_account: 最大账户数量
            hotness: 热点区域占比 (0-1)
            read_hot: 访问热点区域的概率 (0-100)
        """
        self.max_account = max_account
        self.hotness = hotness
        self.read_hot = read_hot
        self.hot_boundary = int(max_account * hotness)
        
    def next(self) -> int:
        """生成下一个符合热点分布的账户号
        
        Returns:
            账户号
        """
        is_hot = random.randint(0, 99) < self.read_hot
        
        if is_hot:
            # 在热点区域生成账户号 [0, hot_boundary)
            return random.randint(0, self.hot_boundary - 1)
        else:
            # 在非热点区域生成账户号 [hot_boundary, max_account)
            return random.randint(self.hot_boundary, self.max_account - 1)

def generate_transactions(max_account: int, hotness: float, read_hot: int, 
                        txn_number: int) -> List[Tuple[int, int]]:
    """生成交易对列表
    
    Args:
        max_account: 最大账户数量
        hotness: 热点区域占比 (0-1)
        read_hot: 访问热点区域的概率 (0-100)
        txn_number: 要生成的交易数量
        
    Returns:
        交易对列表 [(source, dest), ...]
    """
    generator = HotSpotGenerator(max_account, hotness, read_hot)
    transactions = []
    
    for _ in range(txn_number):
        while True:
            source = generator.next()
            dest = generator.next()
            if source != dest:  # 确保源和目标账户不同
                break
        transactions.append((source, dest))
    
    # 打乱交易顺序
    random.shuffle(transactions)
    return transactions

def save_transactions(transactions: List[Tuple[int, int]], filename: str):
    """保存交易到文件
    
    Args:
        transactions: 交易对列表
        filename: 输出文件名
    """
    with open(filename, 'w') as f:
        for source, dest in transactions:
            f.write(f"{source},{dest}\n")

def analyze_transactions(transactions: List[Tuple[int, int]], hot_boundary: int):
    """分析交易的热点分布情况
    
    Args:
        transactions: 交易对列表
        hot_boundary: 热点区域边界
    """
    sources, dests = zip(*transactions)
    all_accounts = sources + dests
    
    hot_accesses = sum(1 for acc in all_accounts if acc < hot_boundary)
    total_accesses = len(all_accounts)
    
    print("\nHot spot analysis:")
    print(f"Hot zone accesses: {hot_accesses} ({hot_accesses/total_accesses*100:.2f}%)")
    print(f"Cold zone accesses: {total_accesses-hot_accesses} ({(total_accesses-hot_accesses)/total_accesses*100:.2f}%)")

def main(hotness: float = 0.2, read_hot: int = 80):
    # 参数设置
    MAX_ACCOUNT = 10000  # 最大账户数量
    TXN_NUMBER = 100000 # 要生成的交易数量
    OUTPUT_FILE = "input/shuffled_transactions.txt"
    
    # 生成并保存交易
    print(f"Generating {TXN_NUMBER} transactions...")
    print(f"Parameters: hotness={hotness}, read_hot={read_hot}%")
    print(f"Hot zone: accounts 0-{int(MAX_ACCOUNT*hotness)-1}")
    print(f"Cold zone: accounts {int(MAX_ACCOUNT*hotness)}-{MAX_ACCOUNT-1}")
    
    transactions = generate_transactions(MAX_ACCOUNT, hotness, read_hot, TXN_NUMBER)
    save_transactions(transactions, OUTPUT_FILE)
    print(f"\nTransactions saved to {OUTPUT_FILE}")
    
    # 打印统计信息
    sources, dests = zip(*transactions)
    print("\nBasic statistics:")
    print(f"Unique source accounts: {len(set(sources))}")