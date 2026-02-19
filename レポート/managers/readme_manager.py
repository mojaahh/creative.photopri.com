#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
README管理スクリプト
各DBや分析のヘッダー項目解説と離脱者定義をまとめたREADMEシートを作成
"""

import os
import sys
import csv
import logging
from datetime import datetime
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('readme_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ReadmeManager:
    """README管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.spreadsheet_uploader = SpreadsheetUploader()
        self.spreadsheet_id = "1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"
    
    def create_readme_sheet(self):
        """READMEシートを作成"""
        try:
            logger.info("READMEシート作成を開始")
            
            # READMEデータを生成
            readme_data = self.generate_readme_data()
            
            # CSVファイルにエクスポート
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"readme_{timestamp}.csv"
            filepath = os.path.join('exports', filename)
            
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows(readme_data)
            
            logger.info(f"READMEデータをエクスポート: {filename}")
            
            # スプレッドシートにアップロード
            success = self.upload_to_readme_sheet(filepath)
            
            if success:
                logger.info("READMEシートの作成が完了しました")
                return True
            else:
                logger.error("READMEシートのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"READMEシート作成エラー: {e}")
            return False
    
    def generate_readme_data(self):
        """READMEデータを生成"""
        readme_data = []
        
        # タイトル
        readme_data.append(["# Shopify データ分析システム README"])
        readme_data.append([])
        
        # 目次
        readme_data.append(["## 📋 目次"])
        readme_data.append(["1. システム概要"])
        readme_data.append(["2. シート構成"])
        readme_data.append(["3. 注文データ（Orders）"])
        readme_data.append(["4. 顧客データベース（Customer DB）"])
        readme_data.append(["5. ユーザー分析（上位100名）"])
        readme_data.append(["6. サービス別分析"])
        readme_data.append(["7. 離脱者アラート"])
        readme_data.append(["8. 離脱者定義と判定基準"])
        readme_data.append(["9. 使用方法"])
        readme_data.append(["10. よくある質問"])
        readme_data.append([])
        
        # システム概要
        readme_data.append(["## 1. システム概要"])
        readme_data.append(["このシステムは、Shopifyの複数ストアから注文データと顧客データを取得し、"])
        readme_data.append(["包括的な分析とアラート機能を提供します。"])
        readme_data.append([])
        readme_data.append(["### 対象ストア"])
        readme_data.append(["• Photopri (photopri.myshopify.com)"])
        readme_data.append(["• Artgraph (artgraph-shop.myshopify.com)"])
        readme_data.append(["• E1 Print (e1print.myshopify.com)"])
        readme_data.append(["• Qoo (aad872-2.myshopify.com)"])
        readme_data.append(["• TETTE (tette-flower.myshopify.com)"])
        readme_data.append([])
        
        # シート構成
        readme_data.append(["## 2. シート構成"])
        readme_data.append(["| シート名", "説明", "更新頻度"])
        readme_data.append(["|---------|------|--------"])
        readme_data.append(["| Orders", "全注文データ", "週次"])
        readme_data.append(["| Customer DB", "全顧客データベース", "週次"])
        readme_data.append(["| ユーザー分析", "上位100名顧客分析", "週次"])
        readme_data.append(["| Photopri分析", "Photopri上位50名分析", "週次"])
        readme_data.append(["| Artgraph分析", "Artgraph上位50名分析", "週次"])
        readme_data.append(["| E1 Print分析", "E1 Print上位50名分析", "週次"])
        readme_data.append(["| Qoo分析", "Qoo上位50名分析", "週次"])
        readme_data.append(["| TETTE分析", "TETTE上位50名分析", "週次"])
        readme_data.append(["| 離脱者アラート", "離脱リスク顧客リスト", "週次"])
        readme_data.append([])
        
        # 注文データ
        readme_data.append(["## 3. 注文データ（Orders）"])
        readme_data.append(["### 主要項目"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| Name", "注文番号", "#P12345"])
        readme_data.append(["| Email", "顧客メールアドレス", "customer@example.com"])
        readme_data.append(["| Created at", "注文日時", "2025/01/15 14:30:00"])
        readme_data.append(["| Total", "注文金額", "¥15,000"])
        readme_data.append(["| Vendor", "ストア名", "Photopri"])
        readme_data.append(["| Lineitem quantity", "商品数量", "3"])
        readme_data.append(["| Lineitem name", "商品名", "フォトブック A4"])
        readme_data.append(["| Lineitem price", "単価", "¥5,000"])
        readme_data.append(["| Lineitem total", "小計", "¥15,000"])
        readme_data.append([])
        
        # 顧客データベース
        readme_data.append(["## 4. 顧客データベース（Customer DB）"])
        readme_data.append(["### 基本情報"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| Customer ID", "Shopify顧客ID", "1234567890"])
        readme_data.append(["| First Name", "名", "太郎"])
        readme_data.append(["| Last Name", "姓", "田中"])
        readme_data.append(["| Email", "メールアドレス", "tanaka@example.com"])
        readme_data.append(["| Phone", "電話番号", "090-1234-5678"])
        readme_data.append(["| Total Spent", "総購入金額", "¥150,000"])
        readme_data.append(["| Total Orders", "総注文数", "25"])
        readme_data.append([])
        
        readme_data.append(["### 住所情報"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| Default Address Address1", "住所1", "東京都渋谷区1-1-1"])
        readme_data.append(["| Default Address City", "市区町村", "渋谷区"])
        readme_data.append(["| Default Address Province Code", "都道府県コード", "JP-13"])
        readme_data.append(["| Default Address Country Code", "国コード", "JP"])
        readme_data.append(["| Default Address Zip", "郵便番号", "150-0001"])
        readme_data.append([])
        
        readme_data.append(["### 統合サービス情報"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| Total Service Orders", "全サービス総注文数", "25"])
        readme_data.append(["| Total Service Amount", "全サービス総購入金額", "¥150,000"])
        readme_data.append(["| First Order Date", "初回注文日", "2023/01/15 10:30:00"])
        readme_data.append(["| Last Order Date", "最終注文日", "2025/01/15 14:30:00"])
        readme_data.append(["| Average Order Interval (Days)", "平均注文間隔（日）", "30.5"])
        readme_data.append(["| Average Order Value", "平均注文単価", "¥6,000"])
        readme_data.append([])
        
        readme_data.append(["### 最終注文情報"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| Last Order ID", "最終注文ID", "1234567890"])
        readme_data.append(["| Last Order Name", "最終注文番号", "#P12345"])
        readme_data.append(["| Last Order Status", "最終注文ステータス", "fulfilled"])
        readme_data.append(["| Last Order Fulfillment Status", "最終注文配送ステータス", "fulfilled"])
        readme_data.append(["| Last Order Amount", "最終注文金額", "¥15,000"])
        readme_data.append([])
        
        readme_data.append(["### 分析フィールド"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| Is New Customer", "新規顧客フラグ", "False"])
        readme_data.append(["| Store Key", "ストアキー", "PHOTOPRI_SHOP"])
        readme_data.append(["| Store Name", "ストア名", "Photopri"])
        readme_data.append(["| Store URL", "ストアURL", "https://photopri.myshopify.com"])
        readme_data.append([])
        
        # ユーザー分析
        readme_data.append(["## 5. ユーザー分析（上位100名）"])
        readme_data.append(["### 対象"])
        readme_data.append(["• 過去3年間に最終注文がある顧客のみ"])
        readme_data.append(["• 総購入金額上位100名"])
        readme_data.append([])
        
        readme_data.append(["### 分析指標"])
        readme_data.append(["| 項目名", "説明", "計算方法"])
        readme_data.append(["|--------|------|--------"])
        readme_data.append(["| Recency (Days)", "最新購入日からの経過日数", "現在日 - 最終注文日"])
        readme_data.append(["| Frequency", "総注文数", "Total Service Orders"])
        readme_data.append(["| Monetary", "総購入金額", "Total Service Amount"])
        readme_data.append(["| Customer Lifespan (Days)", "顧客継続期間", "最終注文日 - 初回注文日"])
        readme_data.append(["| Churn Risk Score", "離脱リスクスコア", "0-100（高いほどリスク大）"])
        readme_data.append(["| Customer Value Score", "顧客価値スコア", "金額×頻度×期間の複合指標"])
        readme_data.append(["| Engagement Score", "エンゲージメントスコア", "注文間隔の短さを評価"])
        readme_data.append(["| Orders per Month", "月間注文数", "総注文数 ÷ (継続期間÷30.4)"])
        readme_data.append(["| Days Since Last Order", "最終注文からの経過日数", "Recency (Days)と同じ"])
        readme_data.append([])
        
        readme_data.append(["### 判定フラグ"])
        readme_data.append(["| 項目名", "説明", "判定基準"])
        readme_data.append(["|--------|------|--------"])
        readme_data.append(["| Is High Value", "高額顧客判定", "総購入金額10万円以上"])
        readme_data.append(["| Is Frequent Buyer", "頻繁購入者判定", "総注文数10回以上"])
        readme_data.append(["| Is At Risk", "離脱リスク判定", "Churn Risk Score70%以上"])
        readme_data.append(["| Customer Type", "顧客タイプ分類", "VIP顧客、頻繁購入者、新規顧客、一般顧客"])
        readme_data.append([])
        
        # サービス別分析
        readme_data.append(["## 6. サービス別分析"])
        readme_data.append(["### 対象"])
        readme_data.append(["• 各ストア（サービス）ごとの上位50名"])
        readme_data.append(["• サービス固有の分析指標を追加"])
        readme_data.append([])
        
        readme_data.append(["### サービス固有指標"])
        readme_data.append(["| 項目名", "説明", "例"])
        readme_data.append(["|--------|------|-----"])
        readme_data.append(["| {サービス名} Recency (Days)", "サービス内最新購入日からの経過日数", "30"])
        readme_data.append(["| {サービス名} Frequency", "サービス内総注文数", "15"])
        readme_data.append(["| {サービス名} Monetary", "サービス内総購入金額", "¥75,000"])
        readme_data.append(["| {サービス名} Rank", "サービス内ランキング", "5"])
        readme_data.append(["| {サービス名} Lifespan (Days)", "サービス利用期間", "365"])
        readme_data.append(["| {サービス名} Growth", "成長度", "急成長、順調成長、緩やか成長、低成長"])
        readme_data.append(["| {サービス名} Dependency (%)", "サービス依存度", "100.0"])
        readme_data.append(["| {サービス名} Churn Risk", "サービス内離脱リスク", "25"])
        readme_data.append(["| {サービス名} Value Score", "サービス価値スコア", "85.5"])
        readme_data.append(["| {サービス名} Monthly Frequency", "月間購入頻度", "1.2"])
        readme_data.append(["| {サービス名} Avg Purchase Interval", "平均購入間隔", "30.5"])
        readme_data.append(["| {サービス名} Customer Type", "サービス内顧客タイプ", "Photopri VIP顧客"])
        readme_data.append(["| {サービス名} Predicted LTV", "予測サービスLTV", "¥200,000"])
        readme_data.append(["| {サービス名} Satisfaction Score", "サービス満足度スコア", "8.5"])
        readme_data.append([])
        
        # 離脱者アラート
        readme_data.append(["## 7. 離脱者アラート"])
        readme_data.append(["### 対象カテゴリ"])
        readme_data.append(["| カテゴリ", "条件", "リスクスコア閾値"])
        readme_data.append(["|--------|------|--------"])
        readme_data.append(["| high_value", "10万円以上、5回以上注文", "60点以上"])
        readme_data.append(["| medium_value", "5万円以上、3回以上注文", "50点以上"])
        readme_data.append(["| frequent_buyer", "3万円以上、10回以上注文", "40点以上"])
        readme_data.append([])
        
        readme_data.append(["### アラートレベル"])
        readme_data.append(["| レベル", "リスクスコア", "対応優先度"])
        readme_data.append(["|--------|--------|--------"])
        readme_data.append(["| 緊急", "80点以上", "即座に対応"])
        readme_data.append(["| 高", "60-79点", "1週間以内に対応"])
        readme_data.append(["| 中", "40-59点", "2週間以内に対応"])
        readme_data.append(["| 低", "40点未満", "1ヶ月以内に対応"])
        readme_data.append([])
        
        readme_data.append(["### 推奨アクション"])
        readme_data.append(["| カテゴリ", "推奨アクション"])
        readme_data.append(["|--------|--------"])
        readme_data.append(["| high_value", "VIP顧客向け特別キャンペーン、個別フォローアップ"])
        readme_data.append(["| medium_value", "リターンキャンペーン、メール配信"])
        readme_data.append(["| frequent_buyer", "頻繁購入者向け特典、購入促進メール"])
        readme_data.append([])
        
        # 離脱者定義
        readme_data.append(["## 8. 離脱者定義と判定基準"])
        readme_data.append(["### 離脱者とは"])
        readme_data.append(["過去に一定以上の購入実績があり、現在離脱のリスクが高いと判定された顧客のことです。"])
        readme_data.append([])
        
        readme_data.append(["### 判定基準（動的リスクスコアベース）"])
        readme_data.append(["#### 1. 基本条件"])
        readme_data.append(["• 各カテゴリの最低購入金額・注文数を満たす"])
        readme_data.append(["• リスクスコアが閾値を超える"])
        readme_data.append([])
        
        readme_data.append(["#### 2. リスクスコア計算"])
        readme_data.append(["##### 動的閾値（平均注文間隔ベース）"])
        readme_data.append(["• 平均注文間隔×1.5倍を超えた時点でリスク開始"])
        readme_data.append(["• 平均注文間隔×2.25倍で中リスク（+30点）"])
        readme_data.append(["• 平均注文間隔×3倍で高リスク（+50点）"])
        readme_data.append([])
        
        readme_data.append(["##### 固定閾値（平均注文間隔不明時）"])
        readme_data.append(["• 60日超：+5点"])
        readme_data.append(["• 90日超：+15点"])
        readme_data.append(["• 180日超：+30点"])
        readme_data.append(["• 365日超：+50点"])
        readme_data.append([])
        
        readme_data.append(["##### リスク軽減要因"])
        readme_data.append(["• 10回以上注文：-10点"])
        readme_data.append(["• 2年以上の長期間顧客：-15点"])
        readme_data.append(["• 高額顧客（20万円以上）：+10点"])
        readme_data.append(["• 中額顧客（10万円以上）：+5点"])
        readme_data.append([])
        
        readme_data.append(["### 例：長期間スパン顧客への配慮"])
        readme_data.append(["平均注文間隔180日の顧客の場合："])
        readme_data.append(["• 270日（180×1.5）でリスク開始"])
        readme_data.append(["• 405日（180×2.25）で中リスク"])
        readme_data.append(["• 540日（180×3）で高リスク"])
        readme_data.append(["従来の固定120日閾値から大幅に改善"])
        readme_data.append([])
        
        # 使用方法
        readme_data.append(["## 9. 使用方法"])
        readme_data.append(["### 統合実行（推奨）"])
        readme_data.append(["```bash"])
        readme_data.append(["cd /Users/photopriinc/Documents/coding/Shopify\\ bulk\\ task/レポート"])
        readme_data.append(["python3 managers/integrated_data_manager.py weekly"])
        readme_data.append(["```"])
        readme_data.append([])
        
        readme_data.append(["### 個別実行"])
        readme_data.append(["```bash"])
        readme_data.append(["# 注文データのみ"])
        readme_data.append(["python3 managers/integrated_data_manager.py orders"])
        readme_data.append([])
        readme_data.append(["# 顧客データのみ"])
        readme_data.append(["python3 managers/integrated_data_manager.py customers"])
        readme_data.append([])
        readme_data.append(["# ユーザー分析のみ"])
        readme_data.append(["python3 managers/integrated_data_manager.py user_analysis"])
        readme_data.append([])
        readme_data.append(["# サービス別分析のみ"])
        readme_data.append(["python3 managers/integrated_data_manager.py service_analysis"])
        readme_data.append([])
        readme_data.append(["# 離脱者アラートのみ"])
        readme_data.append(["python3 managers/integrated_data_manager.py churn_alert"])
        readme_data.append(["```"])
        readme_data.append([])
        
        readme_data.append(["### 離脱者サマリー確認"])
        readme_data.append(["```bash"])
        readme_data.append(["PYTHONPATH=\"/Users/photopriinc/Documents/coding/Shopify\\ bulk\\ task/レポート\" python3 managers/churn_alert_manager.py summary"])
        readme_data.append(["```"])
        readme_data.append([])
        
        # よくある質問
        readme_data.append(["## 10. よくある質問"])
        readme_data.append([])
        
        readme_data.append(["### Q1: なぜ過去3年間の条件があるのですか？"])
        readme_data.append(["A1: より現実的でアクション可能な分析データを提供するためです。"])
        readme_data.append(["古い顧客データは現在のマーケティング戦略に直結しないため、"])
        readme_data.append(["過去3年間に最終注文がある顧客に絞って分析しています。"])
        readme_data.append([])
        
        readme_data.append(["### Q2: 離脱者判定はどのように行われますか？"])
        readme_data.append(["A2: 各顧客の過去の注文パターン（平均注文間隔）を基にした"])
        readme_data.append(["動的リスクスコアで判定されます。固定の日数閾値ではなく、"])
        readme_data.append(["顧客ごとの購入習慣を考慮した精密な判定を行います。"])
        readme_data.append([])
        
        readme_data.append(["### Q3: 長期間の注文スパンを持つ顧客はどう扱われますか？"])
        readme_data.append(["A3: 平均注文間隔の1.5倍を超えた時点でリスク開始とし、"])
        readme_data.append(["3倍を超えた時点で高リスクと判定します。また、2年以上の"])
        readme_data.append(["長期間顧客や10回以上の注文実績がある顧客はリスクスコアを軽減します。"])
        readme_data.append([])
        
        readme_data.append(["### Q4: データの更新頻度はどのくらいですか？"])
        readme_data.append(["A4: 週次更新を推奨しています。統合実行コマンドで"])
        readme_data.append(["注文データ、顧客データ、各種分析を一括更新できます。"])
        readme_data.append([])
        
        readme_data.append(["### Q5: エラーが発生した場合はどうすればよいですか？"])
        readme_data.append(["A5: ログファイルを確認してください。"])
        readme_data.append(["• integrated_data_manager.log"])
        readme_data.append(["• customer_data_manager.log"])
        readme_data.append(["• churn_alert_manager.log"])
        readme_data.append(["• user_analysis_manager.log"])
        readme_data.append(["• service_analysis_manager.log"])
        readme_data.append([])
        
        readme_data.append(["### Q6: スプレッドシートのURLはどこですか？"])
        readme_data.append(["A6: https://docs.google.com/spreadsheets/d/1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"])
        readme_data.append([])
        
        readme_data.append(["### Q7: カスタム分析を追加したい場合はどうすればよいですか？"])
        readme_data.append(["A7: 各マネージャークラス（customer_data_manager.py、"])
        readme_data.append(["user_analysis_manager.py等）を修正して、新しい分析指標を"])
        readme_data.append(["追加できます。詳細は各ファイルのコメントを参照してください。"])
        readme_data.append([])
        
        readme_data.append(["### Q8: データのバックアップは取られていますか？"])
        readme_data.append(["A8: はい、exportsフォルダにCSVファイルとして保存されています。"])
        readme_data.append(["• customers_all_YYYYMMDD_HHMMSS.csv"])
        readme_data.append(["• orders_all_time_YYYY-MM-DD_to_YYYY-MM-DD_YYYYMMDD_HHMMSS.csv"])
        readme_data.append(["• churn_alert_list_YYYYMMDD_HHMMSS.csv"])
        readme_data.append(["• user_analysis_top100_YYYYMMDD_HHMMSS.csv"])
        readme_data.append(["• service_analysis_{サービス名}_YYYYMMDD_HHMMSS.csv"])
        readme_data.append([])
        
        readme_data.append(["---"])
        readme_data.append(["最終更新日: " + datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')])
        readme_data.append(["システムバージョン: 2.0"])
        readme_data.append(["作成者: AI Assistant"])
        
        return readme_data
    
    def upload_to_readme_sheet(self, csv_filepath):
        """READMEシートにアップロード"""
        try:
            sheet_name = "README"
            
            # CSVファイルを読み込み
            with open(csv_filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                data = list(reader)
            
            # シートの内容をクリア
            self.spreadsheet_uploader.clear_sheet_content(sheet_name)
            logger.info(f"シート '{sheet_name}' の内容をクリアしました")
            
            # データを1列にまとめてアップロード
            single_column_data = []
            for row in data:
                if row and row[0]:  # 空行でない場合
                    single_column_data.append([row[0]])
                else:
                    single_column_data.append([""])
            
            # スプレッドシートにアップロード
            range_name = f"{sheet_name}!A:A"
            body = {
                'values': single_column_data
            }
            
            result = self.spreadsheet_uploader.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"シート '{sheet_name}' に {len(single_column_data)} 行のデータをアップロードしました")
            
            # 書式設定を適用
            self.apply_readme_formatting(sheet_name)
            
            return True
                
        except Exception as e:
            logger.error(f"READMEシートアップロードエラー: {e}")
            return False
    
    def apply_readme_formatting(self, sheet_name):
        """READMEシートの書式設定を適用"""
        try:
            # 書式設定を適用
            self.spreadsheet_uploader.format_sheet(sheet_name, 3)  # 3列で十分
            
            logger.info(f"シート '{sheet_name}' の書式設定を適用しました")
            
        except Exception as e:
            logger.error(f"書式設定エラー: {e}")

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='README管理スクリプト')
    parser.add_argument('action', choices=['create_readme'], 
                       help='実行するアクション')
    
    args = parser.parse_args()
    
    try:
        manager = ReadmeManager()
        
        if args.action == 'create_readme':
            print("📊 READMEシートを作成します...")
            success = manager.create_readme_sheet()
            
            if success:
                print("✅ READMEシートの作成が完了しました")
                print("🔗 スプレッドシートURL: https://docs.google.com/spreadsheets/d/1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g")
            else:
                print("❌ READMEシートの作成に失敗しました")
                sys.exit(1)
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
