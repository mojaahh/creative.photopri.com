<?php
header('Content-Type: text/plain; charset=utf-8');
echo "Checking other Python versions...\n";
$versions = ['python3.6', 'python3.7', 'python3.8', 'python3.9', 'python3.10', 'python3.11'];
foreach ($versions as $v) {
    $output = [];
    exec("$v --version 2>&1", $output, $ret);
    if ($ret === 0) {
        echo "$v: " . implode(" ", $output) . "\n";
    }
}
?>