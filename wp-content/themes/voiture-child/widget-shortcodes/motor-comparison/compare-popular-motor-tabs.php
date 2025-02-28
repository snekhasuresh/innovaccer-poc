<?php
function enqueue_compare_motor_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('compare-popular-car-tabs-css', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-comparison/css/compare-popular-motor-tabs.css');
}


function compare_motor_comparison_page()
{
    enqueue_compare_motor_css();

    if (!defined('ABSPATH')) {
        exit; // Exit if accessed directly
        global $post;
        global $wpdb;
    }

    ob_start();


    // <?php
    // $current_post = get_queried_object();
    global $listing_make;
    $listing_type = '';

    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));
    $current_post = $listing_post[0];


    if ($current_post && $current_post->post_type == 'listing') {
        $listing_id = $current_post->ID;

        // Retrieve the terms associated with the listing
        $terms = wp_get_post_terms($listing_id, 'listing_make');
        if (!empty($terms) && !is_wp_error($terms)) {
            $listing_make = $terms[0]->name; // Assuming a single term or take the first one
        }
        $type_terms = wp_get_post_terms($listing_id, 'listing_type');
        if (!empty($type_terms) && !is_wp_error($type_terms)) {
            $listing_type = $type_terms[0]->name; // Assuming a single term or take the first one
        }
    }
    $recommend_car_models = get_option('recommended_car_models');
    $car_models_data = maybe_unserialize($recommend_car_models);

    $popular_cars_ids = [];

    if (!empty($car_models_data) && is_array($car_models_data)) {
        foreach ($car_models_data as $category => $category_data) {

            if (isset($category_data['car_models']) && is_array($category_data['car_models'])) {

                foreach ($category_data['car_models'] as $model) {

                    if (is_array($model) && isset($model['id']) && isset($model['type'])) {
                        // Collect the model IDs where type = 1
                        if ($model['type'] == 1) {
                            $popular_cars_ids[] = $model['id'];
                        }
                    }
                }
            }
        }
    }

    $popular_cars = [];
    if (!empty($popular_cars_ids)) {
        $popular_cars = get_posts(array(
            'post_type' => 'listing',
            'posts_per_page' => 10,
            'post__in' => $popular_cars_ids,
            'orderby' => 'post__in',
        ));
    }
    $latest_cars = get_posts(array(
        'post_type'      => 'upcoming-car',
        'posts_per_page' => 10,
        'meta_query'     => array(
            array(
                'key'     => 'time_to_launch',
                'compare' => 'EXISTS',
                'type'    => 'DATETIME',
            ),
        ),
        'orderby'  => array(
            'meta_value' => 'DESC',
            'ID'    => 'ASC',
        ),
        'meta_key' => 'time_to_launch',
        'meta_type' => 'DATETIME',
    ));

    $listing_types = get_transient('cached_listing_types');

    // Prepare tabs array
    $tabs = [
        ['id' => 'recommended-multi-popular-content', 'label' => 'Popular Cars'],
        ['id' => 'recommended-multi-latest-content', 'label' => 'Latest Cars'],
    ];
?>
    <div class="recommended-multi-car-tabs">
        <h2 class="recommended-multi-tab-heading">Compare Cars</h2>
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

                <?php display_recommendedmotor_posts_compare_page($popular_cars); ?>
            </div>
            <div id="recommended-multi-latest-content" class="recommended-multi-tab-pane">

                <?php display_recommendedmotor_posts_compare_page($latest_cars); ?>
            </div>
        </div>
    </div>
    <script type="text/javascript">
        function toggleVariants(carID) {
            var variantList = document.getElementById('variant-list-' + carID);
            var toggleButton = document.querySelector('.variant-toggle[data-id="' + carID + '"]');

            // Check if the list is currently visible
            var isCurrentlyVisible = variantList.classList.contains('visible');

            // Hide all variant lists first
            var allVariantLists = document.querySelectorAll('.variant-list');
            allVariantLists.forEach(function(list) {
                list.classList.remove('visible');
            });

            // Reset all toggle icons to down arrow
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
                    initializeSlick(targetPane.querySelector('.single-listing-car-list'));
                });
            });

            // Set the first tab and pane as active by default
            tabs[0].classList.add('active');
            panes[0].classList.add('active');

            // Ensure Slick Slider initializes correctly on the first tab after the page load
            setTimeout(function() {
                initializeSlick(panes[0].querySelector('.single-listing-car-list'));
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
                            breakpoint: 1030,
                            settings: {
                                slidesToShow: 2,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 768,
                            settings: {
                                slidesToShow: 1,
                                slidesToScroll: 1
                            }
                        }
                    ]
                });
            }
        }
    </script>

    <style>
        .recommended-multi-tab-content {
            /* padding: 20px; */
            background-color: #fff !important;
            border-top: none !important;
            border-radius: 0 5px 5px 5px !important;
            margin-top: -1px !important;
            padding-top: 20px !important;
            padding-right: 0px !important;
            padding-bottom: 0px !important;
            padding-left: 0px !important;
        }

        .recommended-multi-tab-content .slick-prev {
            background-color: #ffffff !important;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            z-index: 10;
            position: absolute;
            top: 42% !important;
            transform: translateY(-50%);
            display: flex;
            justify-content: center;
            align-items: center;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
            transition: background-color 0.3s ease, color 0.3s ease;
            border: none;
        }

        .recommended-multi-tab-content .slick-next {
            background-color: #ffffff !important;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            z-index: 10;
            position: absolute;
            top: 42% !important;
            transform: translateY(-50%);
            display: flex;
            justify-content: center;
            align-items: center;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
            transition: background-color 0.3s ease, color 0.3s ease;
            border: none;
        }

        .findnew-car-name {
            font-size: 14px;
            font-weight: bold;
            color: #262626;
            white-space: nowrap;
            font-family: 'Roboto';
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
        }

        .recommended-multi-tab-content .slick-next {
            right: -14px !important;
        }
    </style>
    <?php
}
add_shortcode('compare_popular_motor', 'compare_motor_comparison_page');

function display_recommendedmotor_posts_compare_page($cars)
{
    // import C:\xampp\htdocs\wapcar_prepod_testing2\wp-content\themes\voiture-child\widget-shortcodes\ev\css\ev-car-comparison.css
    wp_enqueue_style('ev-car-comparison-css', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-car-comparison.css');

    $posts = $cars;
    $num_posts = count($posts);
    $compare_cars_array = array();
    for ($i = 0; $i < $num_posts; $i += 2) {
        if (isset($posts[$i]) && isset($posts[$i + 1])) {
            $listing1 = $posts[$i];
            $listing2 = $posts[$i + 1];

            // For Listing 1
            $listing1_id = $listing1->ID;
            $listing1_title = get_the_title($listing1_id);
            $listing1_price = get_motor_price($listing1_id);
            $post_thumbnail_id1 = get_post_thumbnail_id($listing1_id);
            $thumbnail_post1 = get_post($post_thumbnail_id1);
            $listing1_image_guid = $thumbnail_post1 ? $thumbnail_post1->guid : '';

            // For Listing 2
            $listing2_id = $listing2->ID;
            $listing2_title = get_the_title($listing2_id);
            $listing2_price = get_motor_price($listing2_id);
            $post_thumbnail_id2 = get_post_thumbnail_id($listing2_id);
            $thumbnail_post2 = get_post($post_thumbnail_id2);
            $listing2_image_guid = $thumbnail_post2 ? $thumbnail_post2->guid : '';

            $compare_cars_array[] = array(
                'listing1' => array(
                    'id' => $listing1_id,
                    'title' => $listing1_title,
                    'price' => $listing1_price,
                    'image_guid' => $listing1_image_guid,
                ),
                'listing2' => array(
                    'id' => $listing2_id,
                    'title' => $listing2_title,
                    'price' => $listing2_price,
                    'image_guid' => $listing2_image_guid,
                ),
            );
        }
    }

    if (!empty($cars)) {
    ?>

        <div class="recommended_carousel">
            <div class="single-listing-car-list">
                <?php foreach ($compare_cars_array as $compare_cars) : ?>
                    <div class="findnew-comparison-item">
                        <div class="car-comparison">
                            <div class="findnew-compare-card">
                                <div class="findnew-car-image-container">
                                    <img src="<?php echo esc_url($compare_cars['listing1']['image_guid']); ?>" alt="<?php echo esc_attr($compare_cars['listing1']['title']); ?>">
                                </div>
                                <div class="price-and-name">
                                    <div class="findnew-car-name"><?php echo esc_html($compare_cars['listing1']['title']); ?></div>
                                    <div class="findnew-car-price"><?php echo esc_html($compare_cars['listing1']['price']); ?></div>
                                </div>
                            </div>
                            <div class="findnew-vs-container">
                                <span class="findnew-vs-tag">VS</span>
                            </div>
                            <div class="findnew-compare-card">
                                <div class="findnew-car-image-container">
                                    <img src="<?php echo esc_url($compare_cars['listing2']['image_guid']); ?>" alt="<?php echo esc_attr($compare_cars['listing2']['title']); ?>">
                                </div>
                                <div class="price-and-name">
                                    <div class="findnew-car-name"><?php echo esc_html($compare_cars['listing2']['title']); ?></div>
                                    <div class="findnew-car-price"><?php echo esc_html($compare_cars['listing2']['price']); ?></div>
                                </div>
                            </div>
                        </div>
                        <a href=<?php echo esc_url(home_url('/compare-cars')); ?> class="findnew-compare-button">
                            Compare <?php echo esc_html($compare_cars['listing1']['title']); ?> and <?php echo esc_html($compare_cars['listing2']['title']); ?>
                        </a>
                    </div>

                <?php endforeach; ?>
            </div>
        </div>
<?php
    } else {
        echo 'No cars found.';
    }
}


function get_motor_price($listing_id)
{
    // Fetch variants of the car
    $args = array(
        'post_type' => 'variant',
        'posts_per_page' => -1,
        'meta_query' => array(
            array(
                'key' => 'model',
                'value' => '"' . $listing_id . '"', // Correct match with serialized data format
                'compare' => 'LIKE',
            ),
        ),
    );
    $variants = new WP_Query($args);

    if (!empty($variants->posts)) {
        $highest_price = 0;
        $lowest_price = 0;
        foreach ($variants->posts as $variant) {
            if (get_post_meta($variant->ID, 'on_sale', true) == 'Yes') {
                $price = get_post_meta($variant->ID, 'retail_price', true);
                if ($price && $price > $highest_price) {
                    $highest_price = $price;
                }
                if ($price && ($price < $lowest_price || $lowest_price == 0)) {
                    $lowest_price = $price;
                }
            }
        }

        if ($highest_price == $lowest_price && $highest_price != 0) {
            $price = 'RM ' . number_format($lowest_price);
        } else {
            $price = 'RM ' . number_format($lowest_price) . ' - RM ' . number_format($highest_price);
        }
    } else {
        $price = 'N/A';
    }

    return $price;
}
