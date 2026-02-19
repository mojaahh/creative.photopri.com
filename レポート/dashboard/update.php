<?php
header('Content-Type: application/json');

// 認証情報の定義（JS側と共通）
$valid_pws = ["Ghjk5945", "Print23953"];

// POSTデータの取得
$pw = $_POST['pw'] ?? '';
$notify = ($_POST['notify'] === 'true') ? '' : '--no-notify';

if (!in_array($pw, $valid_pws)) {
    echo json_encode(["status" => "error", "message" => "認証に失敗しました。"]);
    exit;
}

// すでに実行中かチェック
$status_file = 'data/status.json';
if (file_exists($status_file)) {
    $status_data = json_decode(file_get_contents($status_file), true);
    if ($status_data && $status_data['status'] === 'running' && (time() - strtotime($status_data['last_updated'])) < 3600) {
        echo json_encode(["status" => "error", "message" => "すでに更新処理が実行中です。"]);
        exit;
    }
}

// ログディレクトリの作成
if (!is_dir('logs')) {
    mkdir('logs', 0755, true);
}

// Pythonスクリプトの実行（バックグラウンド）
$cmd = "python3 weekly_report_system.py --mode run " . $notify . " > logs/web_update.log 2>&1 &";

exec($cmd, $output, $return_var);

echo json_encode([
    "status" => "success",
    "message" => "更新処理を開始しました。数分かかります。",
    "debug_cmd" => $cmd // テスト用、本番では削除推奨
]);
