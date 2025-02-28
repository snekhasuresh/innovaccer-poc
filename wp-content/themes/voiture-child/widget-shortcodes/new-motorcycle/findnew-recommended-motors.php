<?php
if (!defined('ABSPATH')) {
    exit; // Exit if accessed directly
    global $post;
    global $wpdb;
}


ob_start();
if (!function_exists('find_new_recommended_bike')) {
    function find_new_recommended_bike($cars)
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

                        // Determine price range of all variants
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
                            if ($highest_price == $lowest_price && $highest_price != 0) {
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
                                <div style="display:flex; justify-content: center;">
                                    <img style="width: 250px; height: 150px; object-fit: cover;" src="<?php echo get_the_post_thumbnail_url($car->ID, 'medium'); ?>"
                                        alt="<?php echo esc_attr($car->post_title); ?>">
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

                                <!-- Display variant list here -->
                                <div>
                                    <ul id="variant-list-<?php echo esc_attr($car->ID); ?>" class="variant-list" style="display: none;">
                                        <?php foreach ($variants->posts as $variant): ?>
                                            <li><a href="<?php echo esc_url(get_permalink($variant->ID)); ?>">
                                                    <?php echo esc_html($variant->post_title); ?>
                                                </a></li>
                                        <?php endforeach; ?>
                                    </ul>
                                </div>
                            </div>

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
    }
    $latest_cars = get_posts(array(
        'post_type' => 'listing',
        'posts_per_page' => 10,
        'orderby' => 'date',
        'order' => 'DESC',
    ));

    $Related_Brand_Car_Models = get_posts(array(
        'post_type' => 'listing',
        'tax_query' => array(
            array(
                'taxonomy' => 'listing_make',
                'field' => 'name',
                'terms' => $listing_make, // The car make you want to filter by
                'orderby' => 'date',
                'order' => 'DESC',
            ),
        ),
        'posts_per_page' => 10,
    ));

    $related_body_type_cars = get_posts(array(
        'post_type' => 'listing',
        'meta_query' => array(
            array(
                'taxonomy' => 'listing_body_type',
                'field' => 'name',
                'terms' => $listing_type, // The car make you want to filter by
                'orderby' => 'date',
                'order' => 'DESC',
            ),
        ),
        'posts_per_page' => 10,
    ));

    $upcoming_cars = get_posts(array(
        'post_type' => 'listing',
        'meta_query' => array(
            array(
                'key' => 'listing_state',
                'value' => 'upcoming',
                'compare' => 'LIKE',
            ),
        ),
        'posts_per_page' => 10,
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
        ['id' => 'recommended-multi-RM20k-80k-content', 'label' => $listing_make . ' Car Models'],
        ['id' => 'recommended-multi-RMover80k-content', 'label' => 'Top 10 ' . $listing_type . " Cars"],
        ['id' => 'recommended-multi-suv-content', 'label' => 'Updates'],

    ];
    ?>
    <div class="recommended-multi-car-tabs">
        <h2 class="recommended-multi-tab-heading">Recommended Cars</h2>
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

                <?php find_new_recommended_bike($popular_cars); ?>
            </div>
            <div id="recommended-multi-latest-content" class="recommended-multi-tab-pane">

                <?php find_new_recommended_bike($latest_cars); ?>
            </div>
            <div id="recommended-multi-RM20k-80k-content" class="recommended-multi-tab-pane">

                <?php find_new_recommended_bike($Related_Brand_Car_Models); ?>
            </div>
            <div id="recommended-multi-RMover80k-content" class="recommended-multi-tab-pane">
                <?php find_new_recommended_bike($related_body_type_cars); ?>
            </div>
            <div id="recommended-multi-suv-content" class="recommended-multi-tab-pane">
                <?php find_new_recommended_bike($upcoming_cars); ?>
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
                    slidesToShow: 4,
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
    </script>

    <style>
        /* Parent container style */
        .recommended_carousel,
        .car-item {
            position: relative;
            /* Create positioning context for absolute positioning */
            overflow: visible;
            /* Ensure content isn't clipped */
            z-index: 1;

            /* Set z-index to ensure dropdowns appear above other content */
        }

        /* Dropdown styles */
        .car-variant-dropdown {
            position: relative;
            /* Make the parent relative to handle the absolute dropdown */


        }

        .variant-list {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.5s ease-out, opacity 0.5s ease;
            opacity: 0;
            /* Make sure the initial opacity is 0 */
        }

        .variant-list.visible {
            max-height: 1000px;
            /* Set a maximum height large enough to fit the content */
            opacity: 1;
        }

        .variant-toggle i:hover {
            color: #333;
            /* Change icon color on hover */
        }

        .variant-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            cursor: pointer;

            padding: 10px 0;
            position: relative;
        }

        .variant-count {
            font-size: 14px;
            color: #757575;
        }

        .variant-toggle {
            background: none;
            border: none;
            color: #757575;
            font-size: 14px;
            cursor: pointer;
            margin: 0;
            padding: 0;
        }

        .variant-list {
            display: none;
            background-color: #fff;
            list-style: none;
            width: 100%;
            left: 0;
            top: 40px;

            transition: 0.5s ease, opacity 0.5s ease;

        }


        .variant-list.visible {
            display: block !important;
            transition: 0.5s ease, opacity 0.5s ease;

        }


        .variant-list li {
            padding: 5px 10px;
        }

        .variant-list li a {
            color: #1a73e8;
            text-decoration: none;
            font-size: 14px;
        }

        .variant-list li a:hover {
            text-decoration: underline;
        }

        .car-item {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 16px;
            display: flex;
            flex-direction: column;
            align-items: center;
            position: relative;

            background-color: #fff;
        }

        .car-image img {
            width: 100%;
            height: 200px;
            /* Set the desired height */
            object-fit: cover;
            /* Ensures the image maintains its aspect ratio while filling the container */
            border-radius: 4px;
        }

        .badge-new-variant {
            position: absolute;
            top: 8px;
            left: 8px;
            background-color: #feb429;
            color: #fff;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 12px;
        }

        .car-details {
            margin-top: 12px;
        }

        .car-make::before {
            content: "";
            position: absolute;
            left: -8px;
            top: 7px;
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background-color: #f5c34b;
        }

        .car-make {
            color: #757575;
            font-size: 12px;
            margin: 0;
            position: relative;
            left: 9px;
        }

        .car-title {
            font-size: 14px;
            font-weight: bold;
            margin: 0;
            color: #333;
        }

        .car-price {
            margin-bottom: 10px;
            font-size: 14px;
            font-weight: bold;
            color: #576B95;
        }

        .car-button .btn-view-model {
            width: 100%;
            text-align: center;
            font-weight: bold;
            padding: 8px;
            border-radius: 4px;
            background-color: #fff;
            color: #f7c551;
            border: 1px solid #f7c551;
            text-decoration: none;
            display: inline-block;
            transition: background-color 0.3s ease, color 0.3s ease;
        }

        .car-button .btn-view-model:hover {
            color: white !important;
            background-color: #f7c551;
        }


        .variant-count {
            font-size: 14px;
            color: #757575;
            margin-right: 8px;
        }

        .variant-toggle {
            background: none;
            border: none;
            color: #757575;
            font-size: 14px;
            cursor: pointer;
        }

        .recommended-multi-car-tabs {
            width: 100%;
        }

        .slick-arrow {
            top: 130px !important;
            transform: none !important;
        }

        .car_item {
            display: flex;
            flex-direction: column;
            border: 1px solid #e0e0e0;
            width: calc(250px - 30px);
            border-radius: 8px;
            overflow: hidden;
            padding: 15px;
            width: 250px;
            margin: 0 15px;
            position: relative;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }

        .slick-track {
            display: flex;
            gap: 20px;
        }

        /* Slick Previous/Next button styles */
        .slick-prev,
        .slick-next {
            background-color: #feb429;
            border-radius: 50%;
            color: white;
            width: 50px;
            height: 50px;
            z-index: 10;
            /* Ensures buttons are above the content */
            position: absolute;
            /* Ensures buttons are positioned properly */
            top: 50%;
            transform: translateY(-50%);
            /* Vertically center the buttons */
            display: flex;
            justify-content: center;
            align-items: center;
            transition: background-color 0.3s ease, color 0.3s ease;
        }

        /* Slick Dots customization */
        .slick-prev {
            left: -30px;
            /* Adjust the positioning of the left arrow */
        }

        /* Position for the Next arrow */
        .slick-next {
            right: -40px;
            /* Adjust the positioning of the right arrow */
        }

        /* Ensure the arrow buttons don't disappear on hover */
        .slick-prev:hover,
        .slick-next:hover {
            background-color: #071A40;
            /* Slightly darker color on hover */
            color: white;
        }

        .slick-arrow {
            position: absolute;
            top: 50%;
            /* Position the arrows vertically centered */
            transform: translateY(-50%);
            z-index: 20;
            /* Keep it above other elements */
        }

        .recommended_carousal {
            display: flex;
            flex-wrap: nowrap;
            /* Prevents wrapping onto a new line */
            overflow: visible;
            /* Hides any overflowed content */
            position: relative;
            gap: 20px;
        }

        .recommended-multi-car-tabs .recommended-multi-tabs {
            display: flex;
            list-style-type: none;
            padding: 0;
            margin: 0;
            border-bottom: 2px solid #e5e5e5;
            overflow-x: auto;
            /* For horizontal scrolling on smaller screens */
        }

        .recommended-multi-car-tabs .recommended-multi-tabs li {
            margin-right: 30px;
            /* Adjust spacing between tabs */
            position: relative;
        }

        .recommended-multi-car-tabs .recommended-multi-tabs li a {
            padding: 15px 10px;
            background-color: transparent;
            text-decoration: none;
            color: #333;
            font-size: 14px;
            font-weight: bold;
            display: block;
            transition: color 0.3s ease;
            position: relative;
            font-family: 'Roboto';
        }

        .recommended-multi-car-tabs .recommended-multi-tabs li a:hover {
            color: #feb429;
            /* Hover color similar to the design */
            border-bottom: 3px solid #feb429;
            /* Underline on hover */
        }

        .recommended-multi-car-tabs .recommended-multi-tabs li a.active {
            color: #feb429;
            font-weight: 600;
            border-bottom: 3px solid #feb429;
        }

        .recommended-multi-car-tabs .recommended-multi-tabs li+li::before {
            content: "";
            height: 20px;
            /* Adjust this to change the height of the divider */
            width: 1px;
            /* Adjust this to change the thickness of the divider */
            background-color: #e5e5e5;
            /* Color of the divider */
            position: absolute;
            left: -15px;
            /* Positioning the divider correctly between tabs */
            top: 50%;
            /* Centering the divider vertically */
            transform: translateY(-50%);
        }

        .recommended-multi-car-tabs .recommended-multi-tab-content {
            padding: 20px;
            background-color: #fff;
            border-top: none;
            border-radius: 0 5px 5px 5px;
            margin-top: -1px;
            /* Prevents a gap between the active tab and content */
        }

        .recommended-multi-car-tabs .recommended-multi-tab-pane {
            display: none;
        }

        .recommended-multi-car-tabs .recommended-multi-tab-pane.active {
            display: block;
        }

        .recommended-multi-car-list {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }
    </style>
<?php }

add_shortcode('find_new_recommended_bike', 'find_new_recommended_bike');
?>