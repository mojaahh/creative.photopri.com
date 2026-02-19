
import os
import shutil
from pathlib import Path

def prepare_deployment():
    project_root = Path(__file__).resolve().parent.parent
    dist_hosting = project_root / 'firebase_hosting'
    dist_api = project_root / 'firebase_api'
    
    # 1. Hostingの準備
    print("🎨 Preparing Firebase Hosting directory...")
    os.makedirs(dist_hosting, exist_ok=True)
    shutil.copy2(project_root / 'レポート' / 'dashboard' / 'index.html', dist_hosting / 'index.html')
    
    # 2. APIの準備
    print("📦 Preparing Cloud Run API directory...")
    os.makedirs(dist_api, exist_ok=True)
    
    # 必要なフォルダをコピー (core, managers, config, レポート/data)
    folders_to_copy = [
        ('レポート/core', 'core'),
        ('レポート/managers', 'managers'),
        ('レポート/config', 'config'),
        ('レポート/data', 'data')
    ]
    
    for src_rel, dst_rel in folders_to_copy:
        src = project_root / src_rel
        dst = dist_api / dst_rel
        if dst.exists():
            shutil.rmtree(dst)
        if src.exists():
            shutil.copytree(src, dst)
            print(f"  Copied {src_rel} to API folder")

    # .env と credentials.json もAPIフォルダに必要
    if (project_root / '.env').exists():
        shutil.copy2(project_root / '.env', dist_api / '.env')
    if (project_root / 'credentials.json').exists():
        shutil.copy2(project_root / 'credentials.json', dist_api / 'credentials.json')

    # APIのコアファイルをコピー
    api_source = project_root / 'レポート' / 'api_source'
    if api_source.exists():
        for f in ['main.py', 'requirements.txt', 'Dockerfile']:
            if (api_source / f).exists():
                shutil.copy2(api_source / f, dist_api / f)
                print(f"  Copied {f} to API folder")

    print("\n✨ Preparation complete!")
    print("\n--- Next Steps ---")
    print("1. Install Firebase CLI: npm install -g firebase-tools")
    print("2. Login: firebase login")
    print("3. Initialize Project: firebase init hosting")
    print("4. Deploy Frontend: firebase deploy --only hosting")
    print("5. Deploy Backend (Cloud Run):")
    print("   cd firebase_api")
    print("   gcloud run deploy weekly-report-api --source . --region asia-northeast1")

if __name__ == "__main__":
    prepare_deployment()
