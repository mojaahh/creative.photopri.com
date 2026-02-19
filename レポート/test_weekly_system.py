#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
週次レポートシステムのテストスクリプト
各コンポーネントの動作確認を行います
"""

import os
import sys
import logging
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from core.lark_notifier import LarkNotifier
from core.summary_generator import SummaryGenerator
from core.weekly_scheduler import WeeklyScheduler

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_lark_connection():
    """Lark接続テスト"""
    print("\n" + "=" * 50)
    print("🧪 Lark接続テスト")
    print("=" * 50)
    
    try:
        notifier = LarkNotifier()
        
        # 接続テスト
        if notifier.test_connection():
            print("✅ Lark接続テスト成功")
            
            # チャット一覧を取得
            chats = notifier.get_chat_list()
            print(f"📋 利用可能なチャット数: {len(chats)}件")
            
            if chats:
                print("\n📝 最初の5件のチャット:")
                for i, chat in enumerate(chats[:5], 1):
                    print(f"  {i}. {chat.get('name', 'Unknown')} (ID: {chat.get('chat_id', 'Unknown')})")
            
            return True
        else:
            print("❌ Lark接続テスト失敗")
            return False
            
    except Exception as e:
        print(f"❌ Lark接続テストエラー: {e}")
        return False

def test_summary_generation():
    """サマリー生成テスト"""
    print("\n" + "=" * 50)
    print("🧪 サマリー生成テスト")
    print("=" * 50)
    
    try:
        generator = SummaryGenerator()
        
        # サマリーを生成
        summary = generator.generate_weekly_summary()
        
        if summary:
            print("✅ サマリー生成テスト成功")
            print(f"📊 生成日時: {summary.get('generated_at', 'Unknown')}")
            print(f"📅 対象月: {summary.get('year', 'Unknown')}年{summary.get('month', 'Unknown')}月")
            
            # 月間目標
            targets = summary.get('monthly_targets', {})
            if targets:
                print(f"\n🎯 月間目標売上:")
                print(f"  全体: {targets.get('total', 0):,.0f}円")
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in targets:
                        print(f"  {service}: {targets[service]:,.0f}円")
            
            # 月間売上
            sales = summary.get('monthly_sales', {})
            if sales:
                print(f"\n💰 月間売上:")
                total_sales = sales.get('total', {})
                print(f"  全体: {total_sales.get('amount', 0):,.0f}円 ({total_sales.get('orders', 0)}件)")
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in sales:
                        service_data = sales[service]
                        print(f"  {service}: {service_data.get('amount', 0):,.0f}円 ({service_data.get('orders', 0)}件)")
            
            # 週末注文
            weekend = summary.get('weekend_orders', {})
            if weekend:
                print(f"\n📅 週末注文:")
                total_weekend = weekend.get('total', {})
                print(f"  全体: {total_weekend.get('amount', 0):,.0f}円 ({total_weekend.get('orders', 0)}件)")
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in weekend:
                        service_data = weekend[service]
                        print(f"  {service}: {service_data.get('amount', 0):,.0f}円 ({service_data.get('orders', 0)}件)")
            
            return True
        else:
            print("❌ サマリー生成テスト失敗")
            return False
            
    except Exception as e:
        print(f"❌ サマリー生成テストエラー: {e}")
        return False

def test_scheduler_components():
    """スケジューラーコンポーネントテスト"""
    print("\n" + "=" * 50)
    print("🧪 スケジューラーコンポーネントテスト")
    print("=" * 50)
    
    try:
        scheduler = WeeklyScheduler()
        
        # コンポーネントテストを実行
        if scheduler.test_components():
            print("✅ スケジューラーコンポーネントテスト成功")
            return True
        else:
            print("❌ スケジューラーコンポーネントテスト失敗")
            return False
            
    except Exception as e:
        print(f"❌ スケジューラーコンポーネントテストエラー: {e}")
        return False

def test_message_formatting():
    """メッセージフォーマットテスト"""
    print("\n" + "=" * 50)
    print("🧪 メッセージフォーマットテスト")
    print("=" * 50)
    
    try:
        notifier = LarkNotifier()
        
        # テスト用のサマリーデータを作成
        test_summary = {
            'monthly_targets': {
                'total': 1000000,
                '#P': 300000,
                '#E': 250000,
                '#A': 250000,
                '#Q': 200000
            },
            'monthly_sales': {
                'total': {'amount': 750000, 'orders': 150},
                '#P': {'amount': 225000, 'orders': 45},
                '#E': {'amount': 187500, 'orders': 37},
                '#A': {'amount': 187500, 'orders': 37},
                '#Q': {'amount': 150000, 'orders': 31}
            },
            'weekend_orders': {
                'total': {'amount': 50000, 'orders': 10},
                '#P': {'amount': 15000, 'orders': 3},
                '#E': {'amount': 12500, 'orders': 2},
                '#A': {'amount': 12500, 'orders': 3},
                '#Q': {'amount': 10000, 'orders': 2}
            },
            'generated_at': '2024-01-15T09:30:00',
            'month': 1,
            'year': 2024
        }
        
        # メッセージを構築
        message = notifier._build_weekly_report_message(test_summary)
        
        print("✅ メッセージフォーマットテスト成功")
        print("\n📝 生成されたメッセージ:")
        print("-" * 50)
        print(message)
        print("-" * 50)
        
        return True
        
    except Exception as e:
        print(f"❌ メッセージフォーマットテストエラー: {e}")
        return False

def main():
    """メインテスト関数"""
    print("🚀 週次レポートシステムテスト開始")
    print("=" * 60)
    
    test_results = []
    
    # 各テストを実行
    test_results.append(("Lark接続テスト", test_lark_connection()))
    test_results.append(("サマリー生成テスト", test_summary_generation()))
    test_results.append(("スケジューラーコンポーネントテスト", test_scheduler_components()))
    test_results.append(("メッセージフォーマットテスト", test_message_formatting()))
    
    # 結果を表示
    print("\n" + "=" * 60)
    print("📊 テスト結果サマリー")
    print("=" * 60)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ 成功" if result else "❌ 失敗"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n📈 結果: {passed}/{total} テストが成功")
    
    if passed == total:
        print("🎉 全テストが成功しました！システムは正常に動作します。")
        return True
    else:
        print("⚠️ 一部のテストが失敗しました。設定を確認してください。")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n👋 テストを中断しました")
        sys.exit(1)
    except Exception as e:
        print(f"❌ テスト実行エラー: {e}")
        sys.exit(1)
