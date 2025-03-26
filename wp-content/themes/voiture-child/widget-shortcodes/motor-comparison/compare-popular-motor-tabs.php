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

    ob_start();

    $popular_bikes = get_popular_bikes_data();
    $latest_bikes = get_latest_bikes_data();

    if (empty($popular_bikes) && empty($latest_bikes)) {
        return;
    }

    // Prepare tabs array
    $tabs = [
        ['id' => 'recommended-multi-popular-content', 'label' => 'Populer'],
        ['id' => 'recommended-multi-latest-content', 'label' => 'Terbaru'],
    ];

?>
    <div class="recommended-multi-car-tabs">
        <h2 class="recommended-multi-tab-heading wa-title-text">So Sánh Xe Máy Phổ Biến</h2>
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

                <?php display_recommendedmotor_posts_compare_page($popular_bikes); ?>
            </div>
            <div id="recommended-multi-latest-content" class="recommended-multi-tab-pane">

                <?php display_recommendedmotor_posts_compare_page($latest_bikes); ?>
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
            margin-left: -17px !important;
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

        .recommended-multi-tab-content .slick-prev {
            left: -8px !important;
        }

        @media screen and (max-width: 768px) {
            .recommended-multi-tab-content .slick-next {
                display: none !important;
            }

            .recommended-multi-tab-content .slick-prev {
                display: none !important;
            }
        }
    </style>
<?php
}
add_shortcode('compare_popular_motor', 'compare_motor_comparison_page');

function display_recommendedmotor_posts_compare_page($posts)
{
    // import C:\xampp\htdocs\wapcar_prepod_testing2\wp-content\themes\voiture-child\widget-shortcodes\ev\css\ev-car-comparison.css
    wp_enqueue_style('ev-car-comparison-css', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-car-comparison.css');

    $num_posts = count($posts);
    if ($num_posts < 2) {
        return;
    }

    $compare_cars_array = array();
    for ($i = 0; $i < $num_posts; $i += 2) {
        if (isset($posts[$i]) && isset($posts[$i + 1])) {
            $listing1 = $posts[$i];
            $listing2 = $posts[$i + 1];

            // For Listing 1
            $listing1_id = $listing1['id'];
            $listing1_title = $listing1['post_title'];
            $listing1_price = $listing1['price_range'];
            $listing1_image_guid = $listing1['thumbnail_url'];
            $listing1_slug = $listing1['post_name'];

            // For Listing 2
            $listing2_id = $listing2['id'];
            $listing2_title = $listing2['post_title'];
            $listing2_price = $listing2['price_range'];
            $listing2_image_guid = $listing2['thumbnail_url'];
            $listing2_slug = $listing2['post_name'];

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
                'comparison_url' => $listing1_slug . '-vs-' . $listing2_slug
            );
        }
    }

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
                    <a href=<?php echo esc_url(home_url('so-sanh-xe-may/') . $compare_cars['comparison_url']); ?> class="findnew-compare-button">
                        <?php echo esc_html($compare_cars['listing1']['title']); ?> vs <?php echo esc_html($compare_cars['listing2']['title']); ?>
                    </a>
                </div>

            <?php endforeach; ?>
        </div>
    </div>
<?php
}
