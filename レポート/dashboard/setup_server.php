<?php
header('Content-Type: text/plain; charset=utf-8');

echo "🛠 XSERVER Python環境セットアップ\n";
echo "==================================\n";

// ユーザーホームディレクトリの取得（サーバーパスから推測）
$home = preg_replace('/public_html.*/', '', $_SERVER['DOCUMENT_ROOT']);
putenv("HOME=$home");
putenv("PYTHONUSERBASE=$home.local");

// インストールするパッケージ（Python 3.6互換）
$packages = [
    'pandas==1.1.5',
    'google-api-python-client==2.52.0',
    'google-auth-oauthlib==0.4.6',
    'schedule==1.1.0',
    'python-dotenv==0.19.2',
    'requests==2.27.1',
    'numpy==1.19.5'
];

foreach ($packages as $pkg) {
    echo "📦 {$pkg} をインストール中...\n";
    // --user, --no-cache-dir を追加し、HOMEを設定。さらに /etc へのアクセスを避けるための変数を追加。
    $cmd = "export HOME=$home; export PIP_CONFIG_FILE=/dev/null; python3 -m pip install {$pkg} --user --no-cache-dir 2>&1";
    $output = [];
    $return_var = 0;
    exec($cmd, $output, $return_var);

    echo implode("\n", $output) . "\n";
    if ($return_var === 0) {
        echo "✅ {$pkg} のインストールに成功しました。\n";
    } else {
        echo "❌ {$pkg} のインストールに失敗しました。\n";
    }
    echo "----------------------------------\n";
}

echo "\n✨ セットアップが完了しました。\n";
?>