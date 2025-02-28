<?php
if (!defined('ABSPATH')) {
    exit;
}
global $post;

$car_post = get_post($post->ID);

// Get the modelKey from the custom field
$model_key = get_post_meta($post->ID, 'listing-model-code', true);

// Prepare the API URL
// $api_url = 'https://www.wapcar.my/v2/carsome_used_car_info/list_related_used_car_by_page_type?languageCode=my-en&countryCode=my&platform=3&modelKey=' . $model_key . '&pageType=carsome_middle_used_car';
$api_url = 'https://www.wapcar.my/v2/carsome_used_car_info/list_related_used_car_by_page_type?languageCode=my-en&countryCode=my&platform=3&modelKey=honda-hr-v&pageType=carsome_middle_used_car';

// Fetch the API data
$response = wp_remote_get($api_url);

echo '<h3>Used Cars</h3>';
if (is_wp_error($response)) {
    echo '<p>Error fetching used car data.</p>';
} else {
    $data = wp_remote_retrieve_body($response);
    $used_cars = json_decode($data, true);

    if (isset($used_cars['data']['list']) && !empty($used_cars['data']['list'])) {
        echo '<div class="used-cars">';
        foreach ($used_cars['data']['list'] as $car) {
            $car_title = esc_html($car['title'] ?? 'No Title');
            $car_image = esc_url($car['cover'] ?? '');
            $car_price = esc_html($car['priceInfo']['expSellingPrice'] ?? 'No Price');
            $car_mileage = esc_html($car['keyInfo'][0]['value'] ?? 'No Mileage');
            $car_used_life = esc_html($car['keyInfo'][1]['value'] ?? 'No Used Life');
            $car_city = esc_html($car['keyInfo'][2]['value'] ?? 'No City');
            $car_link = esc_url($car['targetHref'] ?? '#');

            echo '<div class="used-car-item">';
            if ($car_image) {
                echo '<img src="' . $car_image . '" alt="' . $car_title . '">';
            }
            echo '<h3>' . $car_title . '</h3>';
            echo '<p>Price: RM ' . $car_price . '</p>';
            echo '<p>Mileage: ' . $car_mileage . '</p>';
            echo '<p>Used Life: ' . $car_used_life . '</p>';
            echo '<p>Location: ' . $car_city . '</p>';
            echo '<a href="' . $car_link . '" target="_blank">View More</a>';
            echo '</div>';
        }
        echo '</div>';
    } else {
        echo '<p>No used cars found.</p>';
    }
    echo '<img src="https://static.wapcar.my/pc/my/images/58314964f1bee57315d4.jpg">';
}
?>
<div>
    <style>
        .used-cars {
            display: flex;
            flex-wrap: wrap;
        }
        .used-car-item {
            border: 1px solid #ddd;
            padding: 10px;
            margin: 10px;
            width: calc(33.333% - 20px);
            box-sizing: border-box;
        }
        .used-car-item img {
            max-width: 100%;
            height: auto;
        }
        .used-car-item a {
            display: inline-block;
            margin-top: 10px;
            color: #0073aa;
            text-decoration: none;
        }
        .used-car-item a:hover {
            text-decoration: underline;
        }
    </style>
</div>
