<?php
/**
 * Loader for query-helper functions.
 * Dynamically includes all PHP files in the query-helper directory.
 */

// Get the current directory of this file
$query_helper_dir = __DIR__;

// Scan the directory for PHP files (excluding this loader)
$files = glob($query_helper_dir . '/*.php');

// Include each PHP file, except for loader.php itself
foreach ($files as $file) {
    if (basename($file) !== 'loader.php') {
        require_once $file;
    }
}

$query_helper_dir = __DIR__;
$directory = $query_helper_dir;
$iterator = new RecursiveIteratorIterator(
    new RecursiveDirectoryIterator($directory, RecursiveDirectoryIterator::SKIP_DOTS)
);


foreach ($iterator as $file) {
    if (preg_match('/\.php$/', $file->getFilename())) {
        require_once $file->getPathname();
    }
}


