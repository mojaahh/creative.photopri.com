# 週次レポートシステム - 使用スクリプト一覧

## 🎯 メインシステム

### 週次レポートシステム（新機能）
- **`weekly_report_system.py`** - メインエントリーポイント
  - スケジューラー開始、即座実行、テスト、Webhookテスト、履歴表示
  - 使用方法: `python3 weekly_report_system.py --mode [schedule|run|test|test-webhook|history]`

### コア機能
- **`core/weekly_scheduler.py`** - 週次スケジューラー
  - 毎週月曜日9:00に自動実行
  - 注文データ更新 → サマリー生成 → Lark通知の流れを管理

- **`core/summary_generator.py`** - サマリー生成器
  - 月間目標売上、月間売上実績、週末注文の統計を生成
  - Google Sheetsからデータを取得してフォーマット

- **`core/lark_webhook_notifier.py`** - Lark Webhook通知
  - Webhook経由でLarkにメッセージを送信
  - 環境変数: `LARK_daily_webhook`

## 📊 データ管理システム

### 統合データ管理
- **`managers/integrated_data_manager.py`** - 統合データ管理
  - 週次更新処理（注文データ、顧客データ、分析データ、離脱者アラート）
  - 使用方法: `python3 managers/integrated_data_manager.py weekly`

### 個別データ管理
- **`managers/order_data_manager.py`** - 注文データ管理
  - Shopifyから注文データを取得・エクスポート・アップロード
  - 使用方法: `python3 managers/order_data_manager.py [all_time|recent_3months|recent_2months]`

- **`managers/customer_data_manager.py`** - 顧客データ管理
  - 顧客の詳細情報取得・更新・メトリクス計算

- **`managers/user_analysis_manager.py`** - ユーザー分析管理
  - 上位100名ユーザー分析の生成

- **`managers/service_analysis_manager.py`** - サービス分析管理
  - サービス別分析データの生成

- **`managers/churn_alert_manager.py`** - 離脱者アラート管理
  - 離脱者アラートリストの生成

## 🔧 コア機能（従来）

### データ取得・エクスポート
- **`core/order_export.py`** - 注文データエクスポート
  - Shopify GraphQL APIを使用して注文データを取得
  - 全店舗対応（ArtGraph、Copy Center Gallery、TETTE、Photopri、E1 Print、Qoo）

- **`core/customer_db_generator.py`** - 顧客データベース生成
  - 顧客の詳細情報を取得・新規顧客フラグ計算・サービス別メトリクス

### スプレッドシート操作
- **`core/spreadsheet_uploader.py`** - スプレッドシートアップロード
  - Google Sheets APIを使用したデータアップロード
  - 大容量データの分割アップロード対応

## 🧪 テスト・デバッグ

### システムテスト
- **`test_weekly_system.py`** - 週次システムテスト
  - Lark接続、サマリー生成、スケジューラーコンポーネントのテスト

### 個別テスト
- **`tests/`** ディレクトリ内の各種テストスクリプト
  - `check_db_rows.py` - DB行数チェック
  - `check_latest_data.py` - 最新データチェック
  - `check_sheets.py` - シートチェック
  - `test_datetime_*.py` - 日時フォーマットテスト

### デバッグ・分析
- **`tools/`** ディレクトリ内の分析ツール
  - `analyze_artgraph_orders.py` - ArtGraph注文分析
  - `export_tette_only.py` - TETTE専用エクスポート
  - `test_export.py` - エクスポートテスト

## 📁 レガシーシステム

### 旧バッチ処理
- **`legacy/batch_upload_orders.py`** - 旧注文バッチアップロード
- **`legacy/batch_upload_customer_db.py`** - 旧顧客DBバッチアップロード
- **`legacy/clear_and_reimport_orders.py`** - 注文データクリア・再インポート
- **`legacy/upload_customer_db.py`** - 顧客DBアップロード
- **`legacy/run_order_export_pipeline.py`** - 旧エクスポートパイプライン
- **`legacy/scheduler.py`** - 旧スケジューラー

## 🔧 設定・ログ

### 設定ファイル
- **`config/requirements.txt`** - 必要なPythonパッケージ
- **`config/env_example.txt`** - 環境変数設定例
- **`config/execution_history.json`** - 実行履歴

### ログファイル
- **`*.log`** - 各コンポーネントのログファイル
- **`logs/`** - ログディレクトリ

## 📊 データファイル

### エクスポートデータ
- **`exports/`** - エクスポートされたCSVファイル
- **`data/exports/`** - データディレクトリ内のエクスポートファイル

### 生成データ
- **`data/`** - 生成されたデータファイル
- **`execution_history.json`** - 実行履歴

## 🚀 使用方法

### 週次レポートシステム（推奨）
```bash
# Webhook接続テスト
python3 weekly_report_system.py --mode test-webhook

# 即座に実行（テスト用）
python3 weekly_report_system.py --mode run

# スケジューラー開始（本番用）
python3 weekly_report_system.py --mode schedule

# システムテスト
python3 weekly_report_system.py --mode test

# 実行履歴表示
python3 weekly_report_system.py --mode history
```

### 個別データ管理
```bash
# 注文データ取得（直近3ヶ月）
python3 managers/order_data_manager.py recent_3months

# 統合データ管理（週次更新）
python3 managers/integrated_data_manager.py weekly

# サマリー生成テスト
python3 -c "from core.summary_generator import SummaryGenerator; g = SummaryGenerator(); print(g.format_weekly_summary(g.generate_weekly_summary()))"
```

## 📋 環境変数

### 必須設定
```env
# Google Sheets API
GOOGLE_SHEETS_CREDENTIALS_FILE=credentials.json
GOOGLE_SHEETS_SPREADSHEET_ID=your_spreadsheet_id

# Lark Webhook
LARK_daily_webhook=https://open.larksuite.com/open-apis/bot/v2/hook/your_webhook_url

# Shopify API（各ストア）
SHOPIFY_ACCESS_TOKEN_ARTGRAPH=your_token
SHOPIFY_ACCESS_TOKEN_PHOTOPRI=your_token
SHOPIFY_ACCESS_TOKEN_E1PRINT=your_token
SHOPIFY_ACCESS_TOKEN_QOO=your_token
SHOPIFY_ACCESS_TOKEN_COPYCENTER=your_token
SHOPIFY_ACCESS_TOKEN_TETTE=your_token
```

## 🎯 推奨ワークフロー

1. **開発・テスト時**
   - `weekly_report_system.py --mode test-webhook` でWebhook接続確認
   - `weekly_report_system.py --mode run` で即座実行テスト

2. **本番運用時**
   - `weekly_report_system.py --mode schedule` でスケジューラー開始
   - 毎週月曜日9:00に自動実行

3. **データ更新時**
   - `managers/integrated_data_manager.py weekly` で手動データ更新
   - または個別に各マネージャーを実行

4. **トラブルシューティング時**
   - `weekly_report_system.py --mode history` で実行履歴確認
   - 各`.log`ファイルでエラーログ確認
