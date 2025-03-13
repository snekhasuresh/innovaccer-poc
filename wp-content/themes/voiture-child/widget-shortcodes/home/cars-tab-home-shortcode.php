<?php
function enqueue_recommended_tabs_css()
{
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'car_tab_demo')) {
        wp_enqueue_style('car-tabs-style', get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/car-tabs-home.css');
        wp_enqueue_script('car-tabs-script', get_stylesheet_directory_uri() . '/widget-shortcodes/home/js/car-tabs-home.js', array(), null, true);
//     }
}
add_action('wp_enqueue_scripts', 'enqueue_recommended_tabs_css');


function display_car_posts($cars)
{
	enqueue_recommended_tabs_css();
    if (empty($cars)) {
        return 'No cars found.';
    }

    $listing_states = [
        'On Sale' => ['label' => '', 'color' => '#32D0C6'],
        'Not On Sale' => ['label' => 'Not On Sale', 'color' => '#AAAAAA'],
        'Upcoming' => ['label' => 'Upcoming', 'color' => '#32D0C6']
    ];

    $top_car_model_data = get_option('top_car_models', []);
    $top_car_model_ids = [];
    foreach ($top_car_model_data as $type => $data) {
        $top_car_model_ids = array_merge($top_car_model_ids, array_column($data['car_models'], 'id'));
    }

    ob_start();
?>
    <div class="home-tab-car-list">
        <?php foreach ($cars as $car):
            $is_hot = in_array($car['id'], $top_car_model_ids);
            $listing_state = get_post_meta($car['id'], 'listing-state', true);
            $state = $is_hot ?
                ['label' => 'ฮิต', 'color' => '#F53030'] :
                $listing_states[$listing_state];
        ?>
            <a href="<?php echo esc_url($car['permalink']); ?>" class="tab-car-item">
                <span class="badge" style="background-color: <?php echo $state['color']; ?>;"><?php echo $state['label']; ?></span>
                <span>
                    <img src="<?php echo esc_attr($car['thumbnail_url']); ?>" alt="<?php echo esc_attr($car['post_title']); ?>" class="fixed-thumbnail">
                </span>
                <span>
                    <p class="wap-home-tab-title"><?php echo esc_html($car['post_title']); ?></p>
                </span>
                <span>
                    <p class="cars-price-hometab"><?php echo esc_html($car['price_range']); ?></p>
                </span>
            </a>
        <?php endforeach; ?>
    </div>
<?php
    return ob_get_clean();
}

function display_car_tab()
{
	enqueue_recommended_tabs_css();
    $popular_cars = get_popular_cars_data();
    $latest_cars = get_latest_cars_data();
    $car_data = get_recommended_cars_data();

    // for testing purpose in pre production
    // $popular_cars = fetch_popular_cars_data_from_db();
    // $latest_cars = fetch_latest_cars_data_from_db();
    // $car_data = fetch_recommended_cars_from_db();

    $car_tabs =  $car_tabs = [
        'Xe phổ biến' => 'popular-content',
        'Xe mới nhất' => 'latest-content',
    ];

    foreach ($car_data as $model_key => $cars) {
//         $normalized_model_key = strtolower(preg_replace('/[^a-zA-Z0-9]+/', '-', $model_key));
        // if $model_key = 'Popular', ignore it
        $normalized_model_key = normalize_tab_key($model_key);
		
        if ($normalized_model_key !== 'popular') {
            $car_tabs[$model_key] = $normalized_model_key . '-content';
        }
    }

    ob_start();

$translate = [
    'More' => 'Nhiều hơn',
];

?>
    <div class="car-tabs">
        <div class="tabs-container">
            <ul class="tabs">
                <?php foreach ($car_tabs as $label => $content_id): ?>
                    <li><a href="#<?php echo esc_attr($content_id); ?>" class="home-tab-link"><?php echo esc_html($label); ?></a></li>
                <?php endforeach; ?>
            </ul>
            <a href="<?php echo home_url('/cars'); ?>" class="more-button">
                <span class="more"><?php echo $translate['More']; ?></span>
                 <svg class="more-button-svg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 512">
                    <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5-12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                </svg>
            </a>
        </div>

        <div class="tab-content">
            <div id="popular-content" class="tab-pane">
                <?php echo display_car_posts($popular_cars); ?>
            </div>
            <div id="latest-content" class="tab-pane">
                <?php echo display_car_posts($latest_cars); ?>
            </div>
            <?php
            foreach ($car_data as $model_key => $cars) {
//                 $normalized_model_key = strtolower(preg_replace('/[^a-zA-Z0-9]+/', '-', $model_key));
                $normalized_model_key = normalize_tab_key($model_key);
                echo '<div id="' . esc_attr($normalized_model_key) . '-content" class="tab-pane">';
                echo display_car_posts($cars);
                echo '</div>';
            }
            ?>
        </div>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('car_tab_demo', 'display_car_tab');

function normalize_tab_key($key)
{
    // Convert non-ASCII characters to ASCII, keeping Thai characters intact
    $key = iconv('UTF-8', 'UTF-8//IGNORE', $key);
    // Replace spaces and non-alphanumeric characters with a hyphen
    $key = preg_replace('/[^ก-๙a-zA-Z0-9]+/u', '-', $key);
    // Trim excess hyphens and make lowercase
    $key = strtolower(trim($key, '-'));
    // Ensure the key doesn't start with a number by prepending 'tab-'
    if (is_numeric(substr($key, 0, 1))) {
        $key = 'tab-' . $key;
    }
    return $key;
}