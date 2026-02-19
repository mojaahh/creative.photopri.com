<?php
header('Content-Type: application/json');

$status_file = 'data/status.json';

if (file_exists($status_file)) {
    echo file_get_contents($status_file);
} else {
    echo json_encode(["status" => "idle", "message" => "待機中"]);
}
