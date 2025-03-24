<?php
function enqueue_recommended_motor_tabs_css()
{
    wp_enqueue_style('motor-tabs-style', get_stylesheet_directory_uri() . '/widget-shortcodes/home/motor/css/motor-tabs-home.css');
    wp_enqueue_script('motor-tabs-script', get_stylesheet_directory_uri() . '/widget-shortcodes/home/motor/js/motor-tabs-home.js', array(), null, true);
}

function get_motor_tab_variant_data($posts)
{
    $cars_data = [];
    foreach ($posts as $post) {
        $motor_id = $post->ID;
        $motor_title = get_the_title($motor_id);
        $post_thumbnail_id = get_post_thumbnail_id($motor_id);
        $thumbnail_post = get_post($post_thumbnail_id);
        $thumbnail_url = $thumbnail_post ? $thumbnail_post->guid : ''; // Use the guid for the image URL

        // Get variants and their prices
        $variants_query = new WP_Query([
            'post_type' => 'motorcycle-variant',
            'posts_per_page' => -1,
            'meta_query' => [
                [
                    'key' => 'model',
                    'value' => $motor_id,
                    'compare' => 'LIKE',
                ],
            ],
        ]);

       	$lowest_price = null;
		$highest_price = null;

		if ($variants_query->have_posts()) {
			while ($variants_query->have_posts()) {
				$variants_query->the_post();

				// Only consider posts that are 'state' = 1 and 'on_sale' = 'Yes'
				if (get_post_meta(get_the_ID(), 'state', true) == 1 && get_post_meta(get_the_ID(), 'on_sale', true) == 'Yes') {
					$price = (float)get_post_meta(get_the_ID(), 'price', true);

					// Skip if the price is 0
					if ($price > 0) {
						$lowest_price = is_null($lowest_price) ? $price : min($lowest_price, $price);
						$highest_price = is_null($highest_price) ? $price : max($highest_price, $price);
					}
				}
			}
			wp_reset_postdata();
		}

		$price_range = !is_null($lowest_price) && !is_null($highest_price) ?
			($lowest_price === $highest_price ?
				format_price_vietnam($lowest_price) :
				format_price_vietnam($lowest_price) . ' - ' . format_price_vietnam($highest_price)
			) : 'Đang cập nhật';

        $make_names = wp_list_pluck(wp_get_post_terms($motor_id, 'make'), 'name');
        $cars_data[] = [
            'id' => $motor_id,
            'post_title' => $motor_title,
            'thumbnail_url' => $thumbnail_url,
            'price_range' => $price_range,
            'make_names' => $make_names,
        ];
    }
    return $cars_data;
}

function display_motor_posts($motors)
{
    enqueue_recommended_motor_tabs_css();
    if (empty($motors)) {
        return 'No cars found.';
    }

    $listing_states = [
        'On Sale' => ['label' => '', 'color' => '#32D0C6'],
        'Not On Sale' => ['label' => 'Not On Sale', 'color' => '#AAAAAA'],
        'Upcoming' => ['label' => 'Upcoming', 'color' => '#32D0C6']
    ];

    $top_bike_model_data = get_option('top_bike_models', []);
    $top_bike_model_ids = [];
    foreach ($top_bike_model_data as $type => $data) {
        $top_bike_model_ids = array_merge($top_bike_model_ids, array_column($data['bike_models'], 'id'));
    }

    ob_start();
?>
    <div class="home-tab-bike-list">
        <?php foreach ($motors as $motor) {
            $is_hot = in_array($motor['id'], $top_bike_model_ids);
            $listing_state = get_post_meta($motor['id'], 'listing_state', true);
            $state = $is_hot ?
                ['label' => 'ฮิต', 'color' => '#F53030'] :
                $listing_states[$listing_state];
        ?>
            <a href="<?php echo esc_url($motor['permalink']); ?>" class="tab-car-item">
                <span class="badge" style="background-color: <?php echo $state['color']; ?>;"><?php echo $state['label']; ?></span>
                <span>
                    <img src="<?php echo esc_attr($motor['thumbnail_url']); ?>" alt="<?php echo esc_attr($motor['post_title']); ?>" class="fixed-thumbnail">
                </span>
                <span>
                    <p class="wap-home-tab-title"><?php echo esc_html($motor['post_title']); ?></p>
                </span>
                <span>
                    <p class="cars-price-hometab"><?php echo esc_html($motor['price_range']); ?></p>
                </span>
            </a>
        <?php } ?>
    </div>
<?php

    return ob_get_clean(); // Return the buffered content
}

function display_motor_tab()
{
    enqueue_recommended_motor_tabs_css();

    $popular_bikes = get_popular_bikes_data();
    $latest_bikes = get_latest_bikes_data();
//     $bike_data = get_recommended_cars_data();

    $motor_tabs = [
        'Xe máy phổ biến' => 'motor-popular-content',
        'Xe máy mới nhất' => 'motor-latest-content',
    ];

//     foreach ($bike_data as $model_key => $bikes) {
//         $normalized_model_key = strtolower(preg_replace('/[^a-zA-Z0-9]+/', '-', $model_key));
//         // if $model_key = 'Popular', ignore it
//         if ($normalized_model_key !== 'popular') {
//             $motor_tabs[$model_key] = $normalized_model_key . '-content';
//         }
//     }

    ob_start();
$translate = [
    'More' => 'Nhiều hơn',
];


?>

    <div class="bike-tabs">
        <div class="tabs-container">
            <ul class="tabs">
                <?php foreach ($motor_tabs as $label => $content_id) : ?>
                    <li><a href="#<?php echo esc_attr($content_id); ?>" class="motor-home-tab-link"><?php echo esc_html($label); ?></a></li>
                <?php endforeach; ?>

            </ul>
            <a href="<?php echo home_url('/motorcycles'); ?>" class="more-button"><?php echo $translate['More']; ?><svg width="10px" height="10px" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 512">
                    <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5-12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                </svg></i></a>

        </div>

        <div class="tab-content">
            <div id="motor-popular-content" class="tab-pane">

                <?php echo display_motor_posts($popular_bikes); ?>
            </div>
            <div id="motor-latest-content" class="tab-pane">

                <?php echo display_motor_posts($latest_bikes); ?>
            </div>
            <?php

//             foreach ($motor_data as $model_key => $motors) {
//                 // $normalized_model_key = strtolower(str_replace(' ', '-', $model_key));
//                 $normalized_model_key = strtolower(preg_replace('/[^a-zA-Z0-9]+/', '-', $model_key));
//                 // print_r($normalized_model_key . '-content');
//                 echo '<div id="' . esc_attr($normalized_model_key) . '-content" class="tab-pane">';
//                 echo display_motor_posts($motors);
//                 echo '</div>';
//             }
            ?>
        </div>
    </div>
<?php
    return ob_get_clean(); // Return the buffered content
}
add_shortcode('motor_tab_demo', 'display_motor_tab');
