<?php
header('Content-Type: text/plain; charset=utf-8');
echo "Searching for python binaries in common locations...\n";
$dirs = ['/usr/bin', '/usr/local/bin', '/opt/bin'];
foreach ($dirs as $dir) {
    echo "--- $dir ---\n";
    $output = [];
    exec("ls $dir/python* 2>&1", $output);
    echo implode("\n", $output) . "\n";
}
?>