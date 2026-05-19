#!/usr/bin/env python3
# ============================================================
# Kafka 模拟实时数据生产者
# 用法: python3 kafka_producer.py --topic user-behavior-log --rate 10
# 需要: pip3 install kafka-python
# ============================================================
import json
import time
import random
import argparse
import threading
from kafka import KafkaProducer
from datetime import datetime

# --- 模拟数据池 ---
CITIES = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '南京']
CATEGORIES = ['电子产品', '服装', '食品', '美妆', '家居']
BEHAVIORS = ['view', 'cart', 'fav', 'buy']
PAYMENTS = ['alipay', 'wechat', 'card']
STATUSES = ['pending', 'paid', 'shipped', 'completed', 'cancelled']

producer = KafkaProducer(
    bootstrap_servers=['master:9092', 'slave1:9092', 'slave2:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1,
    retries=3
)

def generate_behavior():
    return {
        'log_id': random.randint(1000000, 9999999),
        'user_id': random.randint(1, 1000),
        'product_id': random.randint(1, 500),
        'behavior_type': random.choice(BEHAVIORS),
        'behavior_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'session_id': f'sess_{random.randint(10000,99999)}',
        'ip': f'192.168.{random.randint(1,254)}.{random.randint(1,254)}'
    }

def generate_order():
    items = []
    total = 0
    for _ in range(random.randint(1, 5)):
        item = {
            'product_id': random.randint(1, 500),
            'quantity': random.randint(1, 3),
            'unit_price': round(random.uniform(10, 200), 2)
        }
        item['subtotal'] = round(item['quantity'] * item['unit_price'], 2)
        items.append(item)
        total += item['subtotal']
    discount = round(random.uniform(0, 50), 2)
    return {
        'order_id': int(datetime.now().strftime('%Y%m%d%H%M%S') + str(random.randint(100,999))),
        'user_id': random.randint(1, 1000),
        'order_status': random.choice(STATUSES),
        'order_amount': round(total, 2),
        'discount_amount': discount,
        'actual_amount': round(total - discount, 2),
        'payment_method': random.choice(PAYMENTS),
        'order_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'items': items
    }

def produce_behavior(rate):
    print(f"[BehaviorProducer] 启动, 速率: {rate}条/秒")
    while True:
        data = generate_behavior()
        producer.send('user-behavior-log', value=data)
        print(f"  📊 {data['behavior_type']:5s} | user={data['user_id']} | product={data['product_id']}")
        time.sleep(1.0 / rate)

def produce_order(rate):
    print(f"[OrderProducer] 启动, 速率: {rate}条/秒")
    while True:
        data = generate_order()
        producer.send('order-stream', value=data)
        print(f"  🛒 订单 {data['order_id']} | 金额={data['actual_amount']} | {data['order_status']}")
        time.sleep(1.0 / rate)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka 模拟数据生产者')
    parser.add_argument('--topic', default='user-behavior-log', help='Topic名称')
    parser.add_argument('--rate', type=int, default=5, help='每秒生成速率')
    args = parser.parse_args()

    # 先等待 broker 就绪
    print("等待 Kafka broker...")
    time.sleep(3)

    if args.topic == 'order-stream':
        produce_order(args.rate)
    else:
        produce_behavior(args.rate)
