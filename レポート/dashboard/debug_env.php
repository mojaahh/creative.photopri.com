<?php
header('Content-Type: text/plain; charset=utf-8');
echo "Checking environment...\n";
$output = [];
exec("python3 --version 2>&1", $output);
echo "Python version: " . implode("\n", $output) . "\n";

$output = [];
exec("which python3 2>&1", $output);
echo "Python path: " . implode("\n", $output) . "\n";

$output = [];
exec("python3 -m pip --version 2>&1", $output);
echo "Pip version: " . implode("\n", $output) . "\n";

$output = [];
exec("ls -ld /tmp 2>&1", $output);
echo "Tmp dir: " . implode("\n", $output) . "\n";
?>