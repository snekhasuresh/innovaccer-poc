<?php
function enqueue_recommended_motor_css()
{
    wp_enqueue_style('recommended-motors-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/recommended-motor-side-shortcode.css');
    wp_enqueue_script('recommended-motors-script', get_stylesheet_directory_uri() . '/widget-shortcodes/common/js/recommended-motor-side-shortcode.js', array(), null, true);
}
if (!function_exists('get_recommended_motor')) {
    function get_recommended_motor($type = 'popular')
    {
        enqueue_recommended_motor_css();

        // $cache_key = 'recommend_5_popular_bikes';
        // $popular_motors = get_transient($cache_key);

        // if (false === $popular_motors) {
        $recommend_motor_models = get_option('recommended_bike_models');
        $motor_models_data = maybe_unserialize($recommend_motor_models);

        $popular_motors_ids = [];
        $latest_bike_ids = [];
        if (!empty($motor_models_data) && is_array($motor_models_data)) {
            foreach ($motor_models_data as $category => $category_data) {
                if ($type === 'popular') {
                    if (isset($category_data['bike_models']) && is_array($category_data['bike_models'])) {
                        foreach ($category_data['bike_models'] as $model) {
                            if (is_array($model) && isset($model['id']) && isset($model['type'])) {
                                // Collect the model IDs where type = 1
                                if ($model['type'] == 1) {
                                    $popular_motors_ids[] = $model['id'];
                                }
                            }
                        }
                    }
                } else {
                    if ($category === 'Popular' && isset($category_data['bike_models']) && is_array($category_data['bike_models'])) {
                        // Extract only the first 10 models from the popular category
                        $popular_bikes = array_slice($category_data['bike_models'], 0, 10);

                        foreach ($popular_bikes as $model) {
                            if (isset($model['id'])) {
                                $latest_bike_ids[] = $model['id'];
                            }
                        }
                    }
                }
            }
        }
        if ($type === 'popular') {
            $popular_motors = [];
            if (!empty($popular_motors_ids)) {
                $motor_posts = get_posts(array(
                    'post_type'      => 'motorcycle-listing',
                    'posts_per_page' => 5,
                    'post__in'       => $popular_motors_ids,
                    'orderby'        => 'post__in',
                    // 'fields'         => 'ids', // Retrieve only IDs
                ));
            }

            if (!empty($motor_posts)) {
                $popular_motors = get_motor_tab_variant_data($motor_posts);
            }
            // Cache the popular motors data
            // set_transient($cache_key, $popular_motors, HOUR_IN_SECONDS);
            // }

            // Return the cached data
            return $popular_motors;
        } elseif ($type === 'latest') {
            $latest_motors = [];
            if (!empty($latest_bike_ids)) {
                $latest_bike_posts = get_posts(array(
                    'post_type'      => 'motorcycle-listing',
                    'posts_per_page' => 5,
                    'post__in'       => $latest_bike_ids,
                    'orderby'        => 'post__in',
                    // 'fields'         => 'ids', // Retrieve only IDs
                ));
            }

            if (!empty($latest_bike_posts)) {
                $latest_motors = get_motor_tab_variant_data($latest_bike_posts);
            }
            //     set_transient($cache_key, $latest_motors, HOUR_IN_SECONDS);
            // }
            return $latest_motors;
        }
    }
}
function display_motors($motors, $tab_type = 'popular')
{
    enqueue_recommended_motor_css();
    ob_start();

    if (!empty($motors)):
?>
        <ul class="custom-recommended-motor-list">
            <?php foreach ($motors as $motor): ?>
                <li class="custom-recommended-motor-item">
                    <div class="custom-recommended-motor-thumbnail">
                        <?php
                        if ($motor['thumbnail_url']): ?>
                            <img src="<?php echo esc_url($motor['thumbnail_url']); ?>" alt="<?php echo esc_attr($motor['post_title']); ?>">
                        <?php else: ?>
                            <img src="https://via.placeholder.com/80" alt="No Image Available">
                        <?php endif; ?>
                    </div>
                    <div class="custom-recommended-motor-info">
                        <a href="<?php echo get_permalink($motor['id']); ?>" class="custom-motor-title">
                            <?php
                            $trimmed_title = wp_trim_words($motor['post_title'], 2, '...');
                            echo esc_html($trimmed_title);
                            ?>
                        </a>

                        <p class="custom-recommended-motor-price">
                            <?php
                            echo '<span class="custom-recommended-price">' . $motor['price_range'] . '</span>';
                            ?>
                        </p>
                    </div>
                </li>
            <?php endforeach; ?>
        </ul>
    <?php
    else:
        echo '<p>No motors available at the moment.</p>';
    endif;

    return ob_get_clean();
}

function recommended_motors_shortcode()
{
    enqueue_recommended_motor_css();
    // Get popular and latest motors but don't display them immediately
    $popular_motors = get_recommended_motor('popular');
    $latest_motors = get_recommended_motor('latest');

    ob_start();
	$translate = [
    'Berita Terkini' => 'Các mẫu xe máy đề xuất',
	'Populer' => 'Phổ biến',
	'Terbaru' => 'Mới nhất',
	];




    ?>

    <div class="custom-recommended-motors">
        <h2 class="wa-title-text "><?php echo $translate['Berita Terkini']; ?></h2>
        <ul class="custom-recommended-tabs">
            <li class="motor-custom-recommended-tab-link current" data-tab="custom-motor-recommended-tab-1"><?php echo $translate['Populer']; ?>
</li>
            <li class="motor-custom-recommended-tab-link" data-tab="custom-motor-recommended-tab-2"><?php echo $translate['Terbaru']; ?></li>
        </ul>

        <div id="custom-motor-recommended-tab-1" class="motor-custom-recommended-tab-content current">
            <!-- Display only popular motors in this tab -->
            <?php echo display_motors($popular_motors, 'popular'); ?>
        </div>

        <div id="custom-motor-recommended-tab-2" class="motor-custom-recommended-tab-content">
            <!-- Display only latest motors in this tab -->
            <?php echo display_motors($latest_motors, 'latest'); ?>
        </div>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('recommended_motors', 'recommended_motors_shortcode');
