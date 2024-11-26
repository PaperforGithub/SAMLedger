import numpy as np
import random
from typing import List, Tuple

class ZipfGenerator:
    def __init__(self, n: int, theta: float):
        """初始化 Zipf 生成器
        
        Args:
            n: 最大账户数量减1
            theta: Zipf 分布的参数
        """
        self.n = n
        self.theta = theta
        # 计算 zeta 值
        self.zeta_n = self._zeta(n, theta)
        self.zeta_2_theta = self._zeta(2, theta)
        
        # 计算其他参数
        self.alpha = 1.0 / (1.0 - theta)
        self.eta = (1.0 - pow(2.0/n, 1.0-theta)) / (1.0 - self.zeta_2_theta/self.zeta_n)

    def _zeta(self, n: int, theta: float) -> float:
        """计算 Zeta 值
        
        Args:
            n: 上限值
            theta: Zipf 参数
            
        Returns:
            zeta 值
        """
        return sum(1.0/pow(float(i), theta) for i in range(1, n+1))

    def next(self) -> int:
        """生成下一个 Zipf 分布的数字
        
        Returns:
            符合 Zipf 分布的随机数
        """
        u = random.random()
        uz = u * self.zeta_n
        
        if uz < 1.0:
            return 1
        if uz < 1.0 + pow(0.5, self.theta):
            return 2
            
        return 1 + int(self.n * pow(self.eta * u - self.eta + 1.0, self.alpha))

def generate_transactions(theta: float, max_account: int, txn_number: int) -> List[Tuple[int, int]]:
    """生成交易对列表
    
    Args:
        theta: Zipf 分布参数
        max_account: 最大账户数量
        txn_number: 要生成的交易数量
        
    Returns:
        交易对列表 [(source, dest), ...]
    """
    zipf_gen = ZipfGenerator(max_account-1, theta)
    transactions = []
    
    for _ in range(txn_number):
        while True:
            source = zipf_gen.next()
            dest = zipf_gen.next()
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

def main(theta: float = 0.99):
    # 参数设置
    MAX_ACCOUNT = 100000  # 最大账户数量
    TXN_NUMBER = 100000  # 要生成的交易数量
    OUTPUT_FILE = "input/shuffled_transactions.txt"
    
    # 生成并保存交易
    print(f"Generating {TXN_NUMBER} transactions with Zipf theta={theta}...")
    transactions = generate_transactions(theta, MAX_ACCOUNT, TXN_NUMBER)
    save_transactions(transactions, OUTPUT_FILE)
    print(f"Transactions saved to {OUTPUT_FILE}")
    
    # 打印一些统计信息
    sources, dests = zip(*transactions)
    print("\nStatistics:")
    print(f"Unique source accounts: {len(set(sources))}")
    print(f"Unique destination accounts: {len(set(dests))}")
    print(f"Account range: {min(min(sources), min(dests))} - {max(max(sources), max(dests))}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--theta', type=float, default=0.99)
    args = parser.parse_args()
    main(theta=args.theta)