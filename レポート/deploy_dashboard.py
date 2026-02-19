
import os
import ftplib
from dotenv import load_dotenv

def deploy():
    # .env から設定を読み込む（親ディレクトリまたはカレントディレクトリ）
    load_dotenv()
    
    ftp_host = os.getenv('FTP_HOST')
    ftp_user = os.getenv('FTP_USER')
    ftp_pass = os.getenv('FTP_PASS')
    ftp_dir = os.getenv('FTP_DIR')

    if not all([ftp_host, ftp_user, ftp_pass, ftp_dir]):
        print("❌ エラー: .env に FTP 設定が不足しています。")
        return

    print(f"🚀 {ftp_host} に接続しています...")
    
    try:
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_pass)
            print("✅ ログイン成功")
            
            # ディレクトリの移動（存在しない場合は適宜作成が必要になる場合がありますが、今回は既存想定）
            try:
                ftp.cwd(ftp_dir)
            except ftplib.error_perm:
                print(f"❌ エラー: ディレクトリ {ftp_dir} に移動できませんでした。")
                return

            print(f"📂 ディレクトリ: {ftp_dir}")

            # 1. index.html のアップロード
            dashboard_path = os.path.join(os.path.dirname(__file__), 'dashboard', 'index.html')
            if os.path.exists(dashboard_path):
                with open(dashboard_path, 'rb') as f:
                    ftp.storbinary('STOR index.html', f)
                print("✅ index.html をアップロードしました")
            else:
                print("⚠️ index.html が見つかりません")

            # 2. data ディレクトリの作成とデータのアップロード
            try:
                ftp.mkd('data')
                print("📁 サーバー上に 'data' ディレクトリを作成しました")
            except:
                pass # すでに存在する場合はスキップ
            
            ftp.cwd('data')
            
            data_file = os.path.join(os.path.dirname(__file__), 'data', 'weekly_summary_data.js')
            if os.path.exists(data_file):
                with open(data_file, 'rb') as f:
                    ftp.storbinary('STOR weekly_summary_data.js', f)
                print("✅ data/weekly_summary_data.js をアップロードしました")
            else:
                print("⚠️ weekly_summary_data.js が見つかりません")

            print("\n✨ デプロイ完了しました！")
            print(f"🔗 URL: https://creative.photopri.com")

    except Exception as e:
        print(f"❌ デプロイ中にエラーが発生しました: {e}")

if __name__ == "__main__":
    deploy()
