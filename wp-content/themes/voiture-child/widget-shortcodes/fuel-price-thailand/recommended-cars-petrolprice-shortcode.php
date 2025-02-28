<?php
function enqueue_fuel_recommended_cars_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('fuel-news-style', get_stylesheet_directory_uri() . '/widget-shortcodes/uel-price-malaysia/css/recommended-cars-petrolprice-shortcode.css');
}
function recommended_cars_petrolprice_shortcode($atts)
{
    enqueue_fuel_recommended_cars_css();
    ob_start(); // Start output buffering
    global $post, $wpdb;
    $atts = shortcode_atts(
        array(
            'brand_id' => 0,
            'listing_type' => 'all',
        ),
        $atts,
        'recommended_cars_for_petrolprice'
    );
    $brand_id = $atts['brand_id'];

    function display_recommendedcar_posts($cars)
    {
        if (!empty($cars)) {
?>
            <div class="recommended_carousel">
                <div class="car-list">
                    <?php foreach ($cars as $car):
                        // Fetch variants of the car
                        $args = array(
                            'post_type' => 'variant',
                            'posts_per_page' => -1,
                            'meta_query' => array(
                                array(
                                    'key' => 'model',
                                    'value' => '"' . $car->ID . '"', // Correct match with serialized data format
                                    'compare' => 'LIKE',
                                ),
                            ),
                        );
                        $variants = new WP_Query($args);

                        // Determine price range of all variantsx`
                        if (!empty($variants->posts)) {
                            $highest_price = 0;
                            $lowest_price = 0;

                            foreach ($variants->posts as $variant) {

                                $price = get_post_meta($variant->ID, 'retail_price', true);

                                if ($price > $highest_price) {
                                    $highest_price = $price;
                                }
                                if ($price < $lowest_price || $lowest_price == 0) {
                                    $lowest_price = $price;
                                }
                            }
                            if ($highest_price == $lowest_price && $highest_price = ! 0) {
                                $price = 'RM ' . number_format($lowest_price);
                            } else {
                                $price = 'RM ' . number_format($lowest_price) . ' - RM ' . number_format($highest_price);
                            }
                        } else {
                            $price = 'N/A';
                        }
                    ?>
                        <div class="car-item">
                            <a href="<?php echo esc_url(get_permalink($car->ID)); ?>" class="car-link">
                                <div style="width: 230px; height: 170px;">
                                    <?php
                                    // Fetch the post thumbnail ID
                                    $post_thumbnail_id = get_post_thumbnail_id($car->ID);
                                    $thumbnail_post = get_post($post_thumbnail_id);

                                    // Get the GUID of the image
                                    $guid = $thumbnail_post ? $thumbnail_post->guid : '';
                                    ?>
                                    <img src="<?php echo esc_url($guid); ?>" alt="<?php echo esc_attr($car->post_title); ?>">
                                </div>
                                <span class="car-details">
                                    <p class="car-make"><?php echo get_the_term_list($car->ID, 'listing_make', '', ', '); ?></p>
                                    <h4 class="car-title"><?php echo esc_html($car->post_title); ?></h4>
                                </span>
                                <span class="car-price">
                                    <p><?php echo esc_html($price); ?></p>
                                </span>
                                <span class="car-button">
                                    <a href="<?php echo esc_url(get_permalink($car->ID)); ?>" class="btn-view-model">View Model</a>
                                </span>
                            </a>
                            <div class="car-variant-dropdown">
                                <div class="variant-header" onclick="toggleVariants('<?php echo esc_js($car->ID); ?>')">
                                    <span class="variant-count"><?php echo count($variants->posts); ?> Variants</span>
                                    <button class="variant-toggle" data-id="<?php echo esc_attr($car->ID); ?>">
                                        <i class="fas fa-chevron-down"></i> <!-- Font Awesome down icon -->
                                    </button>
                                </div>

                            </div>
                            <?php foreach ($variants->posts as $variant): ?>
                                <ul id="variant-list-<?php echo esc_attr($car->ID); ?>" class="variant-list">
                                    <li><a
                                            href="<?php echo esc_url(get_permalink($variant->ID)); ?>"><?php echo esc_html($variant->post_title); ?></a>
                                    </li>
                                <?php endforeach; ?>
                                </ul>
                        </div>
                    <?php endforeach; ?>
                </div>
            </div>
    <?php
        } else {
            echo 'No cars found.';
        }
    }

    ?>

    <?php
    $current_post = get_queried_object();
    global $listing_make;
    $listing_type = '';

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
    $top_car_model_ids = get_option('top_car_models', []);
    $popular_cars = [];
    if (!empty($top_car_model_ids)) {
        $popular_cars = get_posts(array(
            'post_type' => 'listing',
            'posts_per_page' => -1,
            'post__in' => $top_car_model_ids,
            'orderby' => 'post__in',
        ));
        $listing_args = array(
            'post_type'      => 'listing',
            'posts_per_page' => -1,
            'meta_query'     => array(
                array(
                    'key'     => '_listing_make',
                    'value'   => $brand_id,
                    'compare' => '='
                )
            )
        );
        $listings = get_posts($listing_args);

        $listing_ids = wp_list_pluck($listings, 'ID');
        $meta_query = array('relation' => 'OR');

        foreach ($listing_ids as $listing_id) {
            $meta_query[] = array(
                'key'     => 'related_car_models',
                'value'   => '"' . $listing_id . '"',
                'compare' => 'LIKE'
            );
        }

        $args = array(
            'post_type'      => 'news',
            'posts_per_page' => 3,
            'orderby'        => 'date',
            'order'          => 'DESC',
            'meta_query'     => $meta_query,
        );

        $news_posts = new WP_Query($args);
    }


    $latest_cars = get_posts(array(
        'post_type' => 'listing',
        'posts_per_page' => 10,
        'orderby' => 'date',
        'order' => 'DESC',
    ));

    $current_post = get_queried_object();
    global $listing_make;
    $listing_type = '';

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

    // Prepare tabs array
    $tabs = [
        ['id' => 'recommended-multi-popular-content', 'label' => 'Popular Cars'],
        ['id' => 'recommended-multi-latest-content', 'label' => 'Latest Cars'],
        // ['id' => 'recommended-multi-RM20k-80k-content', 'label' => $listing_make . ' Car Models'],
        // ['id' => 'recommended-multi-RMover80k-content', 'label' => 'Top 10 ' . $listing_type . " Cars"],
        // ['id' => 'recommended-multi-suv-content', 'label' => 'Updates'],

    ];
    ?>
    <div class="recommended-multi-car-tabs">
        <h2 class="recommended-multi-tab-heading wa-title-text">Recommended Cars</h2>
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

                <?php display_recommendedcar_posts($popular_cars); ?>
            </div>
            <div id="recommended-multi-latest-content" class="recommended-multi-tab-pane">

                <?php display_recommendedcar_posts($latest_cars); ?>
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

            // Reset all toggle icons to down
            var allToggleButtons = document.querySelectorAll('.variant-toggle');
            allToggleButtons.forEach(function(button) {
                button.innerHTML = '▼'; // Reset to down arrow
            });

            // If the clicked list was not visible, show it
            if (!isCurrentlyVisible) {
                variantList.classList.add('visible');
                toggleButton.innerHTML = '▲'; // Change to up arrow
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
                    initializeSlick(targetPane.querySelector('.car-list'));
                });
            });

            // Set the first tab and pane as active by default
            tabs[0].classList.add('active');
            panes[0].classList.add('active');

            // Ensure Slick Slider initializes correctly on the first tab after the page load
            setTimeout(function() {
                initializeSlick(panes[0].querySelector('.car-list'));
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
add_shortcode('recommended_cars_for_petrolprice', 'recommended_cars_petrolprice_shortcode');
