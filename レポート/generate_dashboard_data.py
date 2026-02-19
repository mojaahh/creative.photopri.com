
import sys
import os
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from core.summary_generator import SummaryGenerator

def main():
    print("🚀 ダッシュボード用データ生成（データ更新スキップ）")
    
    try:
        generator = SummaryGenerator()
        summary = generator.generate_weekly_summary()
        
        if summary:
            # Check if we can save
            # Manual save to JS
            js_content = f"const weeklySummaryData = {json.dumps(summary, ensure_ascii=False, indent=2)};"
            js_file_path = os.path.join(project_root, 'data', 'weekly_summary_data.js')
            with open(js_file_path, 'w', encoding='utf-8') as f:
                f.write(js_content)
            print(f"✅ JSデータファイルを保存しました: {js_file_path}")
            
            # Also save JSON for reference
            json_file_path = os.path.join(project_root, 'data', 'weekly_summary_latest.json')
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            print(f"✅ JSONデータファイルを保存しました: {json_file_path}")
            
        else:
            print("❌ サマリー生成失敗")
            
    except Exception as e:
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    main()
