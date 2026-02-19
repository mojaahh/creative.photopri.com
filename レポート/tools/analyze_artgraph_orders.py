#!/usr/bin/env python3
"""
ArtGraph注文データの詳細分析スクリプト
"""

import pandas as pd
import os

def analyze_artgraph_orders():
    """ArtGraph注文データを分析"""
    
    # 最新のArtGraph注文ファイルを探す
    exports_dir = "exports"
    artgraph_files = [f for f in os.listdir(exports_dir) if f.startswith('artgraph_orders_list_') and f.endswith('.csv')]
    
    if not artgraph_files:
        print("❌ ArtGraph注文ファイルが見つかりません")
        return
    
    latest_file = max(artgraph_files, key=lambda x: os.path.getctime(os.path.join(exports_dir, x)))
    csv_file_path = os.path.join(exports_dir, latest_file)
    
    print(f"📁 分析対象ファイル: {latest_file}")
    
    # データを読み込み
    df = pd.read_csv(csv_file_path, encoding='utf-8', low_memory=False)
    print(f"📊 総注文数: {len(df)}件")
    
    # 重要な列の情報を分析
    important_cols = [
        'Name', 'Email', 'Billing Name', 'Billing Phone', 'Phone', 
        'Billing Country', 'Billing Zip', 'Billing City', 'Billing Province',
        'Shipping Name', 'Shipping Phone', 'Shipping Country', 'Shipping Zip', 
        'Shipping City', 'Shipping Province', 'Created at', 'Total'
    ]
    
    print("\n📋 重要な列のデータ分析:")
    print("=" * 60)
    
    for col in important_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            null_count = df[col].isna().sum()
            unique_count = df[col].nunique()
            
            print(f"\n{col}:")
            print(f"  非null値: {non_null_count}件 ({non_null_count/len(df)*100:.1f}%)")
            print(f"  null値: {null_count}件 ({null_count/len(df)*100:.1f}%)")
            print(f"  ユニーク値: {unique_count}個")
            
            # 非null値がある場合、サンプルを表示
            if non_null_count > 0:
                sample_values = df[col].dropna().head(3).tolist()
                print(f"  サンプル値: {sample_values}")
    
    # 住所情報の詳細分析
    print("\n🏠 住所情報の詳細分析:")
    print("=" * 40)
    
    billing_address_cols = ['Billing Name', 'Billing City', 'Billing Zip', 'Billing Province', 'Billing Country']
    shipping_address_cols = ['Shipping Name', 'Shipping City', 'Shipping Zip', 'Shipping Province', 'Shipping Country']
    
    print("\n請求先住所:")
    for col in billing_address_cols:
        if col in df.columns:
            non_null = df[col].notna().sum()
            print(f"  {col}: {non_null}件 ({non_null/len(df)*100:.1f}%)")
    
    print("\n配送先住所:")
    for col in shipping_address_cols:
        if col in df.columns:
            non_null = df[col].notna().sum()
            print(f"  {col}: {non_null}件 ({non_null/len(df)*100:.1f}%)")
    
    # 電話番号の分析
    print("\n📞 電話番号の分析:")
    print("=" * 30)
    
    phone_cols = ['Billing Phone', 'Shipping Phone', 'Phone']
    for col in phone_cols:
        if col in df.columns:
            non_null = df[col].notna().sum()
            if non_null > 0:
                sample_phones = df[col].dropna().head(5).tolist()
                print(f"  {col}: {non_null}件 ({non_null/len(df)*100:.1f}%)")
                print(f"    サンプル: {sample_phones}")
            else:
                print(f"  {col}: 0件 (0.0%)")
    
    # 完全な顧客情報を持つ注文の分析
    print("\n👤 完全な顧客情報を持つ注文:")
    print("=" * 40)
    
    # メール + 氏名 + 電話番号 + 住所の組み合わせで分析
    has_email = df['Email'].notna()
    has_billing_name = df['Billing Name'].notna()
    has_phone = df['Billing Phone'].notna() | df['Shipping Phone'].notna() | df['Phone'].notna()
    has_address = df['Billing City'].notna() | df['Shipping City'].notna()
    
    complete_info = has_email & has_billing_name & has_phone & has_address
    print(f"  完全な情報を持つ注文: {complete_info.sum()}件 ({complete_info.sum()/len(df)*100:.1f}%)")
    
    # 部分的な情報を持つ注文
    partial_info = has_email & (has_billing_name | has_phone | has_address)
    print(f"  部分的な情報を持つ注文: {partial_info.sum()}件 ({partial_info.sum()/len(df)*100:.1f}%)")
    
    # メールのみの注文
    email_only = has_email & ~has_billing_name & ~has_phone & ~has_address
    print(f"  メールのみの注文: {email_only.sum()}件 ({email_only.sum()/len(df)*100:.1f}%)")

if __name__ == "__main__":
    analyze_artgraph_orders()
