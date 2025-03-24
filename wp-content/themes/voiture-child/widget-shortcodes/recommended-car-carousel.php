<?php
function enqueue_recommended_car_carousal_css()
{
//     wp_enqueue_style('recommended-cars-style', get_stylesheet_directory_uri() . '/widget-shortcodes/recommended-car-carousel.css');
	
	wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('single-listing-recommended_cars', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/single-listing-recommended-cars.css');
}

function recommended_cars_horizontal_shortcode($atts)
{
    enqueue_recommended_car_carousal_css();
// 	enqueue_single_listing_recommended_cars_css();
	
    ob_start(); // Start output buffering
    $atts = shortcode_atts(
        array(
            'brand_id' => 0,
            'listing_type' => 'all',
        ),
        $atts,
        'recommended_cars_horizontal'
    );

    $popular_cars = get_popular_cars_data(true);
    $latest_cars = get_latest_cars_data(true);

    $updates = get_posts(array(
        'post_type' => 'listing',
        'posts_per_page' => 10,
        'orderby' => 'date',
        'order' => 'DESC',
        'fields' => 'ids',
    ));

    $updates = format_car_response($updates, true);

    // Prepare tabs array
    $tabs = [
        ['id' => 'recommended-multi-popular-content', 'label' => 'Phổ biến'],
        ['id' => 'recommended-multi-latest-content', 'label' => 'Mới nhất'],
    ];
    if (is_page('tools')) {
        $tabs[] = ['id' => 'recommended-multi-update-content', 'label' => 'Cập Nhật'];
    }
?>
    <div class="recommended-multi-car-tabs">
        <?php
        if (is_page('tools')) {
        ?>
            <h2 class="recommended-multi-tab-heading wa-title-text">Xe phổ biến</h2>
        <?php
        } else {
        ?>
            <h2 class="recommended-multi-tab-heading wa-title-text">Các mẫu xe đề xuất</h2>
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
                <?php display_recommendedcar_posts_common($popular_cars, 'recommended-multi-popular-content'); ?>
            </div>
            <div id="recommended-multi-latest-content" class="recommended-multi-tab-pane">
                <?php display_recommendedcar_posts_common($latest_cars, 'recommended-multi-latest-content'); ?>
            </div>
            <div id="recommended-multi-update-content" class="recommended-multi-tab-pane">
                <?php display_recommendedcar_posts_common($updates, 'recommended-multi-update-content'); ?>
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
                            breakpoint: 1030,
                            settings: {
                                slidesToShow: 2,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 768,
                            settings: {
                                slidesToShow: 1.2,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 480,
                            settings: {
                                slidesToShow: 1.2,
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
add_shortcode('recommended_cars_horizontal', 'recommended_cars_horizontal_shortcode');

function display_recommendedcar_posts_common($cars)
{
    if (empty($cars)) {
        return;
    }

    $listing_states = [
        'On Sale' => ['label' => ' Nóng ', 'color' => '#F53030'],
        'Not On Sale' => ['label' => 'Not On Sale', 'color' => '#AAAAAA'],
        'Upcoming' => ['label' => 'Sắp ra mắt', 'color' => '#32D0C6']
    ];
    $top_car_model_data = get_option('top_car_models', []);
    $all_car_models = $top_car_model_data;

    $top_car_model_ids = array();
    foreach ($all_car_models as $type => $data) {
        $top_car_model_ids = array_merge($top_car_model_ids, array_column($data['car_models'], 'id'));
    }
?>
    <div class="recommended_carousel">
        <div class="wap-car-list">
            <?php foreach ($cars as $car):
                $post_id = $car['id'];
                $post_title = $car['post_title'];
                $post_name = $car['post_name'];
                $image_guid = $car['thumbnail_url'];
                $price = $car['price_range'];
                $listing_make = $car['listing_make'];
                $listing_state = $car['listing_state'];
                $permalink = $car['permalink'];
                $variants = $car['variants'];
                $variants_count = count($variants);

                // Prepare the base URL for the car model
                $home_url = get_home_url();
                $make = strtolower(str_replace(' ', '-', $listing_make));
                $model = strtolower(str_replace(' ', '-', $post_name));
	
				// Remove the make from the model if it exists
				if (strpos($model, $make) === 0) { // Check if the model starts with the make
					$model = trim(str_replace($make, '', $model), '-');
				}
                $base_url = $home_url . '/xe-oto/' . $make . '/' . $model . '/';

                // Check if the car is hot
                $is_hot = false;
                if (in_array($post_id, $top_car_model_ids)) {
                    $is_hot = true;
                }
                $state = $is_hot ? ['label' => 'Nóng', 'color' => '#F53030'] : $listing_states[$listing_state];
            ?>
                <div class="wa-single-car-item">
                    <a href="<?php echo esc_url($permalink); ?>" class="car-link">
                        <span class="badge" style="background-color: <?php echo $state['color']; ?>;"><?php echo $state['label']; ?></span>
                        <div style="display:flex; justify-content: center;">
                            <img style="width: 100%; height: 150px; object-fit: cover;" src="<?php echo esc_url($image_guid); ?>"
                                alt="<?php echo esc_attr($post_title); ?>">
                        </div>
                        <span class="car-details">
                            <p class="car-make"><?php echo $listing_make; ?></p>
                            <h4 class="car-title"><?php echo $post_title; ?></h4>
                        </span>
                        <span class="car-price">
                            <p><?php echo $price; ?></p>
                        </span>
                        <span class="car-button">
                            <a href="<?php echo esc_url($permalink); ?>" class="btn-view-model"> Xem dòng xe </a>
                        </span>
                    </a>


                    <div class="car-variant-dropdown">
                        <div class="variant-header" onclick="toggleVariants('<?php echo esc_js($post_id); ?>')">
                            <span class="variant-count"><?php echo $variants_count; ?>   mẫu xe </span>
                            <button class="variant-toggle" data-id="<?php echo esc_attr($post_id); ?>">
                                <i class="fas fa-chevron-down"></i> <!-- Font Awesome down icon -->
                            </button>
                        </div>

                        <!-- Display variant list here -->
                        <div>
                            <ul id="variant-list-<?php echo esc_attr($post_id); ?>" class="variant-list" style="display: none;">
                                <?php foreach ($variants as $variant): ?>
                                    <li><a href="<?php echo $base_url . $variant['post_name']; ?>">
                                            <?php echo $variant['title']; ?>
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
}
