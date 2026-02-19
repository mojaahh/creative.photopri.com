import os
import sys
import threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# プロジェクトルートを追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# .env を読み込み
load_dotenv()

app = Flask(__name__)
CORS(app)

# 遅延インポート（依存関係エラーを避けるため）
def get_scheduler():
    from core.weekly_scheduler import WeeklyScheduler
    return WeeklyScheduler()

@app.route("/")
def hello():
    return "Weekly Report API is running."

@app.route("/run_update", methods=["POST"])
def run_update():
    # 簡易的なAPIキー認証
    api_key = request.headers.get("X-API-Key")
    if api_key != os.getenv("API_AUTH_TOKEN"):
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    notify = request.args.get("notify", "true").lower() == "true"
    
    # 非同期で実行開始
    def task():
        try:
            scheduler = get_scheduler()
            scheduler.run_immediately(notify=notify)
        except Exception as e:
            print(f"Background task error: {e}")

    thread = threading.Thread(target=task)
    thread.start()

    return jsonify({
        "status": "success",
        "message": "Update process started in background."
    })

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
