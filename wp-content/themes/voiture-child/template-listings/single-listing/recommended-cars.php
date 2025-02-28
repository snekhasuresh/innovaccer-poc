<?php
function enqueue_single_listing_recommended_cars_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('single-listing-recommended_cars', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/single-listing-recommended-cars.css');
}


function single_listing_recommended_cars()
{
    enqueue_single_listing_recommended_cars_css();

    ob_start();

    $global_listing_post_data = get_listing_from_query_vars();
    $listing_make = $global_listing_post_data['listing_make_term']->name;
    $post_meta = $global_listing_post_data['post_meta'];
    $listing_type_id = $post_meta['_listing_type'][0];
    $listing_type_term = get_term_by('id', $listing_type_id, 'listing_type');
    $listing_type = $listing_type_term->name;

    $popular_cars = get_popular_cars_data(true);
    $latest_cars = get_latest_cars_data(true);

    // get post ids of the related car models
    $related_brand_car_models = get_posts(array(
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
    $formatted_related_brand_car_models = format_car_response_by_posts($related_brand_car_models, true);

    $related_body_type_cars = get_posts(array(
        'post_type' => 'listing',
        'meta_query' => array(
            array(
                'key' => '_listing_type',
                'value' => strval($listing_type_id),
                'compare' => '='
            ),
        ),
        'posts_per_page' => 10,
    ));
    $formatted_related_body_type_cars = format_car_response_by_posts($related_body_type_cars, true);

    $upcoming_cars = get_posts(array(
        'post_type' => 'listing',
        'posts_per_page' => 10,
        'orderby' => 'date',
        'order' => 'DESC',
    ));
    $formatted_upcoming_cars = format_car_response_by_posts($upcoming_cars, true);

    // Prepare tabs array
    $tabs = [
        ['id' => 'recommended-multi-popular-content', 'label' => ' ยอดนิยม '],
        ['id' => 'recommended-multi-latest-content', 'label' => ' ล่าสุด '],
        ['id' => 'recommended-multi-RM20k-80k-content', 'label' => ' รุ่นรถ ' . $listing_make],
        ['id' => 'recommended-multi-RMover80k-content', 'label' => $listing_type . " อันดับรถซีดาน "],
        ['id' => 'recommended-multi-suv-content', 'label' => 'อัพเดท'],

    ];
?>
    <div class="recommended-multi-car-tabs">
        <h2 class="recommended-multi-tab-heading wa-title-text">รถแนะนำสำหรับคุณ</h2>
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
            <div id="recommended-multi-RM20k-80k-content" class="recommended-multi-tab-pane">

                <?php display_recommendedcar_posts($formatted_related_brand_car_models); ?>
            </div>
            <div id="recommended-multi-RMover80k-content" class="recommended-multi-tab-pane">
                <?php display_recommendedcar_posts($formatted_related_body_type_cars); ?>
            </div>
            <div id="recommended-multi-suv-content" class="recommended-multi-tab-pane">
                <?php display_recommendedcar_posts($formatted_upcoming_cars); ?>
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
    </script>

    <style>

    </style>
<?php
}
add_shortcode('single_listing_recommended_cars', 'single_listing_recommended_cars');

function display_recommendedcar_posts($cars)
{
    if (empty($cars)) {
        return;
    }

    $listing_states = [
        'On Sale' => ['label' => 'On Sale', 'color' => '#F53030'],
        'Not On Sale' => ['label' => 'Not On Sale', 'color' => '#AAAAAA'],
        'Upcoming' => ['label' => 'Upcoming', 'color' => '#32D0C6']
    ];
    $top_car_model_data = get_option('top_car_models', []);
    $all_car_models = $top_car_model_data;

    $top_car_model_ids = array();
    foreach ($all_car_models as $type => $data) {
        $top_car_model_ids = array_merge($top_car_model_ids, array_column($data['car_models'], 'id'));
    }
?>
    <div class="recommended_carousel">
        <div class="single-listing-car-list">
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
                $base_url = $home_url . '/cars/' . $make . '/' . $model . '/';

                // Check if the car is hot
                $is_hot = false;
                if (in_array($post_id, $top_car_model_ids)) {
                    $is_hot = true;
                }
                $state = $is_hot ? ['label' => 'Hot', 'color' => '#F53030'] : $listing_states[$listing_state];
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
                            <a href="<?php echo esc_url($permalink); ?>" class="btn-view-model">View Model</a>
                        </span>
                    </a>


                    <div class="car-variant-dropdown">
                        <div class="variant-header" onclick="toggleVariants('<?php echo esc_js($post_id); ?>')">
                            <span class="variant-count"><?php echo $variants_count; ?> รุ่นย่อย </span>
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
?>