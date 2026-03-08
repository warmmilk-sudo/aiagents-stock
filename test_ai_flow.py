import os
from dotenv import load_dotenv
import logging
from smart_monitor_deepseek import SmartMonitorDeepSeek
from smart_monitor_data import SmartMonitorDataFetcher

load_dotenv()
logging.basicConfig(level=logging.INFO)

def test_labeling_and_ai():
    fetcher = SmartMonitorDataFetcher()
    data = fetcher.get_comprehensive_data('600519') # 贵州茅台
    
    if not data:
        print("Failed to fetch data")
        return
        
    print(f"Data fetched successfully. Semantic labels: {data.get('semantic_labels')}")
    
    # 模拟账户
    account_info = {'available_cash': 100000, 'total_value': 100000, 'positions_count': 0}
    
    engine = SmartMonitorDeepSeek(api_key=os.getenv('DEEPSEEK_API_KEY'))
    print("Sending to AI...")
    result = engine.analyze_stock_and_decide('600519', data, account_info)
    
    if result['success']:
        print("--- AI Decision ---")
        print(f"Action: {result['decision']['action']}")
        print(f"Confidence: {result['decision']['confidence']}")
        print(f"Reasoning: {result['decision']['reasoning']}")
    else:
        print(f"AI Failed: {result['error']}")

if __name__ == '__main__':
    test_labeling_and_ai()
