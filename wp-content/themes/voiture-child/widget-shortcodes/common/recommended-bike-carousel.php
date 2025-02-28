<?php
function enqueue_recommended_bike_carousal_css()
{
    wp_enqueue_style('recommended-cars-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/recommended-car-carousel.css');
}

function get_motor_variant_data($posts)
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
                if (get_post_meta(get_the_ID(), 'state', true) == 1 && get_post_meta(get_the_ID(), 'on_sale', true) == 'Yes') {
                    $price = (float)get_post_meta(get_the_ID(), 'price', true);
                    $lowest_price = is_null($lowest_price) ? $price : min($lowest_price, $price);
                    $highest_price = is_null($highest_price) ? $price : max($highest_price, $price);
                }
            }
            wp_reset_postdata();
        }

        $price_range = !is_null($lowest_price) && !is_null($highest_price) ?
            ($lowest_price === $highest_price ?
                'THB ' . number_format($lowest_price) :
                'THB ' . number_format($lowest_price) . ' - THB ' . number_format($highest_price)
            ) : 'ยังไม่คอนเฟิร์ม';

        $make_names = wp_list_pluck(wp_get_post_terms($motor_id, 'listing_make'), 'name');
        $cars_data[] = [
            'id' => $motor_id,
            'post_title' => $motor_title,
            'thumbnail_url' => $thumbnail_url,
            'price_range' => $price_range,
            'make_names' => $make_names,
            'variants' => $variants_query,
        ];
    }
    return $cars_data;
}

function recommended_bikes_horizontal_shortcode($atts)
{
    enqueue_recommended_bike_carousal_css();
    ob_start(); // Start output buffering
    global $post, $wpdb;
    $atts = shortcode_atts(
        array(
            'brand_id' => 0,
            'listing_type' => 'all',
        ),
        $atts,
        'recommended_bikes_horizontal'
    );
    $brand_id = $atts['brand_id'];

    function display_recommended_bike($motors, $tab_id)
    {
        if (!empty($motors)) {
?>
            <div class="recommended_carousel">
                <div class="wap-car-list">
                    <?php foreach ($motors as $motor) {
                        $badges = ['VR', 'Hot']; // Array of badges

                        $random_badge = $badges[array_rand($badges)];

                        $badge_class = $random_badge == 'Hot' ? 'badge-hot' : 'badge-vr';
                    ?>
                        <div class="wa-car-item">
                            <a href="<?php echo esc_url(get_permalink($motor['id'])); ?>" class="car-link">
                                <span class="badge <?php echo esc_attr($badge_class); ?>"><?php echo esc_html($random_badge); ?></span>
                                <div class="recommended-car-image-container">
                                    <img src="<?php echo esc_attr($motor['thumbnail_url']); ?>" alt="<?php echo esc_attr($motor['post_title']); ?>" class="fixed-thumbnail">
                                </div>
                                <span class="car-details">
                                    <p class="car-make"><?php echo esc_html($motor['make_names']); ?></p>
                                    <h4 class="car-title"><?php echo  esc_html($motor['post_title']); ?></h4>
                                </span>
                                <span class="car-price">
                                    <p><?php echo esc_html($motor['price_range']); ?></p>
                                </span>
                                <span class="car-button">
                                    <a href="<?php echo esc_url(get_permalink($motor['id'])); ?>" class="btn-view-model">ดูรุ่นรถ</a>
                                </span>
                            </a>
                            <div class="car-variant-dropdown">
                                <div class="variant-header" onclick="toggleVariants('<?php echo esc_js($motor['id']); ?>', '<?php echo esc_js($tab_id); ?>')">
                                    <span class="variant-count"><?php echo count($motor['variants']); ?> รุ่นย่อย</span>
                                    <button class="variant-toggle" data-id="<?php echo esc_attr($motor['id']); ?>" data-tab="<?php echo esc_attr($tab_id); ?>"><i class="fas fa-chevron-down"></i></button>
                                </div>
                            </div>

                            <?php

                            foreach ($motor['variants']->posts as $variant):
                            ?>
                                <ul id="variant-list-<?php echo esc_attr($motor['id']) . '-' . esc_attr($tab_id); ?>" class="variant-list">
                                    <li><a
                                            href="<?php echo esc_url(get_permalink($variant->ID)); ?>"><?php echo esc_html($variant->post_title); ?></a>
                                    </li>
                                <?php endforeach; ?>
                                </ul>
                        </div>
                    <?php } ?>
                </div>
            </div>
    <?php
        } else {
            echo 'No cars found.';
        }
    }

    ?>

    <?php
    $cache_key = 'motor_popular_bike_carousel';
    $popular_bikes = get_transient($cache_key);

    if (false === $popular_bikes) {
        $popular_bikes_ids = [];

        $recommend_bike_models = get_option('recommended_bike_models');
        $motor_models_data = maybe_unserialize($recommend_bike_models);

        if (!empty($motor_models_data) && is_array($motor_models_data)) {
            foreach ($motor_models_data as $category => $category_data) {

                if (isset($category_data['bike_models']) && is_array($category_data['bike_models'])) {

                    foreach ($category_data['bike_models'] as $model) {

                        if (is_array($model) && isset($model['id']) && isset($model['type'])) {
                            // Collect the model IDs where type = 1
                            if ($model['type'] == 1) {
                                $popular_bikes_ids[] = $model['id'];
                            }
                        }
                    }
                }
            }
        }

        $popular_bikes = [];
        if (!empty($popular_bikes_ids)) {
            $motor_posts = get_posts(array(
                'post_type' => 'motorcycle-listing',
                'posts_per_page' => 10,
                'post__in' => $popular_bikes_ids,
                'orderby' => 'post__in',
            ));

            if (!empty($motor_posts)) {
                $popular_bikes = get_motor_variant_data($motor_posts);
            }
        }
        set_transient($cache_key, $popular_bikes, HOUR_IN_SECONDS);
    }


    //cache for latest bikes
    $cache_key = 'motor_latest_bikes_carousel';
    $latest_bikes = get_transient($cache_key);

    if (false === $latest_bikes) {

        $latest_bikes = [];
        $latest_bike_posts = get_posts(array(
            'post_type' => 'motorcycle-listing',
            'posts_per_page' => 10,
            'orderby' => 'date', // Ensure posts are ordered by date
            'order' => 'DESC', // Retrieve the latest posts
        ));
        if (!empty($latest_bike_posts)) {
            $latest_bikes = get_motor_variant_data($latest_bike_posts);
        }
        set_transient($cache_key, $latest_bikes, HOUR_IN_SECONDS);
    }



    //cache for latest bikes
    $cache_key = 'motor_latest_bikes_carousel';
    $updates = get_transient($cache_key);

    if (false === $updates) {

        $updates = [];
        $update_bike_posts = get_posts(array(
            'post_type' => 'motorcycle-listing',
            'posts_per_page' => 10,
            'orderby' => 'date', // Ensure posts are ordered by date
            'order' => 'DESC', // Retrieve the latest posts
        ));
        if (!empty($update_bike_posts)) {
            $updates = get_motor_variant_data($latest_bike_posts);
        }
        set_transient($cache_key, $updates, HOUR_IN_SECONDS);
    }

    // Prepare tabs array
    $tabs = [
        ['id' => 'recommended-multi-popular-content', 'label' => 'ยอดนิยม'],
        ['id' => 'recommended-multi-latest-content', 'label' => 'ล่าสุด'],
    ];
    if (is_page('motor-loan-calculator')) {
        $tabs[] = ['id' => 'recommended-multi-update-content', 'label' => 'อัพเดท'];
    }
    ?>
    <div class="recommended-multi-car-tabs">
        <?php
        if (is_page('motor-loan-calculator')) {
        ?>
            <h2 class="recommended-multi-tab-heading wa-title-text">รถจักรยานยนต์ยอดนิยม</h2>
        <?php
        } else {
        ?>
            <h2 class="recommended-multi-tab-heading wa-title-text">รถแนะนำสำหรับคุณ</h2>
        <?php
        }
        ?>
        <ul class="recommended-multi-tabs">
            <?php foreach ($tabs as $index => $tab): ?>
                <li>
                    <a href="#<?php echo $tab['id']; ?>"
                        class="recommended-multi-tab-link <?php echo $index === 0 ? 'active' : ''; ?>">
                        <?php echo $tab['label']; ?>
                    </a>
                </li>
            <?php endforeach; ?>
        </ul>
        <div class="recommended-multi-tab-content">
            <div id="recommended-multi-popular-content" class="recommended-multi-tab-pane">
                <?php display_recommended_bike($popular_bikes, 'recommended-multi-popular-content'); ?>
            </div>
            <div id="recommended-multi-latest-content" class="recommended-multi-tab-pane">
                <?php display_recommended_bike($latest_bikes, 'recommended-multi-latest-content'); ?>
            </div>
            <div id="recommended-multi-update-content" class="recommended-multi-tab-pane">
                <?php display_recommended_bike($updates, 'recommended-multi-update-content'); ?>
            </div>

        </div>
    </div>

    <script type="text/javascript">
        function toggleVariants(carID, tabID) {
            var variantList = document.getElementById('variant-list-' + carID + '-' + tabID);
            var toggleButton = document.querySelector('.car-variant-dropdown .variant-toggle[data-id="' + carID + '"][data-tab="' + tabID + '"]');

            // Check if the list is currently visible
            var isCurrentlyVisible = variantList.classList.contains('visible');

            // Hide all variant lists first
            var allVariantLists = document.querySelectorAll('.variant-list');
            allVariantLists.forEach(function(list) {
                list.classList.remove('visible');
            });

            var allToggleButtons = document.querySelectorAll('.variant-toggle');
            allToggleButtons.forEach(function(button) {
                button.innerHTML = '<i class="fas fa-chevron-down"></i>'; // Reset to down arrow icon
            });

            // If the clicked list was not visible, show it
            if (!isCurrentlyVisible) {
                variantList.classList.add('visible');
                toggleButton.innerHTML = '<i class="fas fa-chevron-up"></i>'; // Change to up arrow icon
            }
        }


        document.addEventListener('DOMContentLoaded', function() {
            // Tab Switching Logic
            const tabs = document.querySelectorAll('.recommended-multi-car-tabs .recommended-multi-tabs a');
            const panes = document.querySelectorAll('.recommended-multi-car-tabs .recommended-multi-tab-pane');

            tabs.forEach(function(tab) {
                tab.addEventListener('click', function(event) {
                    event.preventDefault();

                    // Remove active class from all tabs and panes
                    tabs.forEach(t => t.classList.remove('active'));
                    panes.forEach(p => p.classList.remove('active'));

                    // Add active class to the clicked tab and corresponding pane
                    tab.classList.add('active');
                    const targetPane = document.querySelector(tab.getAttribute('href'));
                    targetPane.classList.add('active');

                    // Initialize Slick for the newly active tab
                    initializeSlick(targetPane.querySelector('.wap-car-list'));
                });
            });

            // Set the first tab and pane as active by default
            tabs[0].classList.add('active');
            panes[0].classList.add('active');

            // Ensure Slick Slider initializes correctly on the first tab after the page load
            setTimeout(function() {
                initializeSlick(panes[0].querySelector('.wap-car-list'));
            }, 200); // Small delay to allow the first tab UI to settle
        });

        // Function to initialize Slick Slider
        function initializeSlick(selector) {
            if (selector && !jQuery(selector).hasClass('slick-initialized')) {
                jQuery(selector).slick({
                    slidesToShow: 3,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,

                    responsive: [{
                            breakpoint: 1024,
                            settings: {
                                slidesToShow: 4,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 600,
                            settings: {
                                slidesToShow: 2,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 480,
                            settings: {
                                slidesToShow: 1,
                                slidesToScroll: 1
                            }
                        }
                    ]
                });
            }
        }
        $(document).ready(function() {
            $('.slick-track').css({
                'gap': '7px',
            });
        });
    </script>


<?php
    return ob_get_clean(); // Return the buffered output as the shortcode content
}

// Register the shortcode
add_shortcode('recommended_bikes_horizontal', 'recommended_bikes_horizontal_shortcode');
