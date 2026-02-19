<?php
header('Content-Type: text/plain; charset=utf-8');
echo "Checking pre-installed packages...\n";
$packages = ['pandas', 'googleapiclient', 'google_auth_oauthlib', 'schedule', 'dotenv', 'requests', 'numpy'];
foreach ($packages as $pkg) {
    $output = [];
    exec("python3 -c \"import $pkg; print('Found $pkg')\" 2>&1", $output, $ret);
    if ($ret === 0) {
        echo "✅ $pkg is available\n";
    } else {
        echo "❌ $pkg is NOT available\n";
    }
}
?>