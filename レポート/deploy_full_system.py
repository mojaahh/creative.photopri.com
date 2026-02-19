
import os
import ftplib
import shutil
from dotenv import load_dotenv

def deploy():
    # .env から設定を読み込む
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
            
            try:
                ftp.cwd(ftp_dir)
            except ftplib.error_perm:
                print(f"❌ エラー: ディレクトリ {ftp_dir} に移動できませんでした。")
                return

            print(f"📂 ディレクトリ: {ftp_dir}")

            def upload_dir(local_path, remote_path):
                try:
                    ftp.mkd(remote_path)
                except:
                    pass
                
                for item in os.listdir(local_path):
                    if item.startswith('.') or item == 'venv' or item == '__pycache__' or item == 'logs' or item == 'exports':
                        continue
                    
                    l_path = os.path.join(local_path, item)
                    r_path = remote_path + "/" + item if remote_path else item
                    
                    if os.path.isfile(l_path):
                        with open(l_path, 'rb') as f:
                            ftp.storbinary(f'STOR {r_path}', f)
                        print(f"  Uploaded: {r_path}")
                    elif os.path.isdir(l_path):
                        upload_dir(l_path, r_path)

            # 1. ルートの重要ファイルをアップロード
            root_files = ['.env', 'credentials.json']
            for f_name in root_files:
                if os.path.exists(f_name):
                    with open(f_name, 'rb') as f:
                        ftp.storbinary(f'STOR {f_name}', f)
                    print(f"✅ {f_name} をアップロードしました")

            # 2. レポートディレクトリの中身をアップロード
            report_dir = 'レポート'
            print("📦 レポートシステムのファイルをアップロード中...")
            for item in os.listdir(report_dir):
                if item.startswith('.') or item == 'venv' or item == '__pycache__' or item == 'logs' or item == 'exports':
                    continue
                
                l_path = os.path.join(report_dir, item)
                
                if os.path.isfile(l_path):
                    # dashboardの中身はルートに出すか、そのまま出すか
                    # 現状 dashboard/index.html をルートに置いているので、dashboardフォルダ自体はスルーして中身を後でやる？
                    # いや、dashboardフォルダの中に進捗チェックPHPとか置くので、そのまま上げても良い。
                    # ただし、index.html はルートに必要。
                    with open(l_path, 'rb') as f:
                        ftp.storbinary(f'STOR {item}', f)
                elif os.path.isdir(l_path):
                    upload_dir(l_path, item)

            # 3. dashboard の中身をルートに展開（index.html, PHPなど）
            dashboard_dir = os.path.join(report_dir, 'dashboard')
            if os.path.exists(dashboard_dir):
                print("🎨 ダッシュボードファイルをルートに展開中...")
                for item in os.listdir(dashboard_dir):
                    if item.startswith('.'): continue
                    l_path = os.path.join(dashboard_dir, item)
                    if os.path.isfile(l_path):
                        with open(l_path, 'rb') as f:
                            ftp.storbinary(f'STOR {item}', f)
                        print(f"  Dashboard: {item}")

            print("\n✨ システム全体のデプロイが完了しました！")
            print(f"🔗 URL: https://creative.photopri.com")

    except Exception as e:
        print(f"❌ デプロイ中にエラーが発生しました: {e}")

if __name__ == "__main__":
    deploy()
