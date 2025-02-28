<?php
// Get the cookie value if it's set
$vehicle_type = isset($_COOKIE['vehicleType']) ? $_COOKIE['vehicleType'] : 'car'; // Default to 'car'

// Get the current page URL path (e.g., /cars or /motorcycle)
$page_path = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');

// Check if the URL path is '/cars' or '/motorcycle'
if ($page_path == 'cars') {
    // If the page is /cars, show the cars shortcode
    if ($vehicle_type == 'car') {
        echo do_shortcode('[hfe_template id="26932"]');
    } else {
        echo 'Cookie does not match the page!';
    }
} elseif ($page_path == 'motorcycle') {
    // If the page is /motorcycle, show the motorcycle shortcode
    if ($vehicle_type == 'motorcycle') {
        echo do_shortcode('[hfe_template id="26940"]');
    } else {
        echo 'Cookie does not match the page!';
    }
} else {
    // For other pages, show a default shortcode
    echo do_shortcode('[default_shortcode]');
}
?>
