<?php
function enqueue_ev_comparison_css()
{
    wp_enqueue_style('ev-comparison-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-car-comparison.css');
}

function initialize_ev_comparison_findnew_slider()
{
?>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            var itemCount = $('.findnew-comparison-carousel .findnew-comparison-item').length;
            var slidesToShow = Math.min(itemCount, 3); // Show up to 3 items

            if (itemCount > 0) {
                $('.findnew-comparison-carousel').slick({
                    slidesToShow: slidesToShow,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,
                    cssEase: 'ease',
                    responsive: [{
                            breakpoint: 1024,
                            settings: {
                                slidesToShow: slidesToShow,
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
        });
    </script>
<?php
}
add_action('wp_footer', 'initialize_ev_comparison_findnew_slider');

function ev_car_comparison($atts)
{
    enqueue_ev_comparison_css();
    $car_posts = get_ev_car_comparison_data();

    if (empty($car_posts)) {
        return '';
    }

    ob_start();
?>
    <div class="popular-ev-head">
        <h2 class="wa-title-text">Ev Car Comparison</h2>
    </div>
    <div class="findnew-comparison-wrapper">
        <div class="findnew-comparison-carousel">
            <?php
            $num_posts = count($car_posts);

            for ($i = 0; $i < $num_posts; $i += 2) :
                if (isset($car_posts[$i]) && isset($car_posts[$i + 1])) :
                    $listing1 = $car_posts[$i];
                    $listing2 = $car_posts[$i + 1];

                    // For Listing 1
                    $listing1_title = $listing1->post_title;
                    $listing1_price = $listing1->price_range;
                    $listing1_url = $listing1->permalink;
                    $listing1_slug = $listing1->post_name;
                    $listing1_image_guid = $listing1->thumbnail_url;

                    $price_parts = explode(' - ', $listing1_price);
                    $listing1_price = $price_parts[0];

                    // For Listing 2
                    $listing2_title = $listing2->post_title;
                    $listing2_price = $listing2->price_range;
                    $listing2_url = $listing2->permalink;
                    $listing2_slug = $listing2->post_name;
                    $listing2_image_guid = $listing2->thumbnail_url;

                    $price_parts = explode(' - ', $listing2_price);
                    $listing2_price = $price_parts[0];

                    $comparison_slug = $listing1_slug . '-vs-' . $listing2_slug;
            ?>
                    <div class="findnew-comparison-item">
                        <div class="car-comparison">
                            <div class="findnew-compare-card">
                                <a href="<?php echo $listing1_url; ?>" class="findnew-car-image-container">
                                    <img src="<?php echo esc_url($listing1_image_guid); ?>" alt="<?php echo esc_attr($listing1_title); ?>">
                                </a>
                                <div class="price-and-name">
                                    <a href="<?php echo $listing1_url; ?>" class="findnew-car-name"><?php echo esc_html($listing1_title); ?></a>
                                    <div class="findnew-car-price"><?php echo esc_html($listing1_price); ?></div>
                                </div>
                            </div>
                            <div class="findnew-vs-container">
                                <span class="findnew-vs-tag">VS</span>
                            </div>
                            <div class="findnew-compare-card">
                                <a href="<?php echo $listing2_url; ?>" class="findnew-car-image-container">
                                    <img src="<?php echo esc_url($listing2_image_guid); ?>" alt="<?php echo esc_attr($listing2_title); ?>">
                                </a>
                                <div class="price-and-name">
                                    <a href="<?php echo $listing2_url; ?>" class="findnew-car-name"><?php echo $listing2_title; ?></a>
                                    <div class="findnew-car-price"><?php echo esc_html($listing2_price); ?></div>
                                </div>
                            </div>
                        </div>
                        <a href="<?php echo home_url('/compare-cars/') . $comparison_slug; ?>" class="findnew-compare-button">
                            Compare <?php echo esc_html($listing1_title); ?> and <?php echo esc_html($listing2_title); ?>
                        </a>
                    </div>
            <?php
                endif; // End check for a valid pair
            endfor; // End for loop
            wp_reset_postdata();
            ?>
        </div>
    </div>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            if ($('.findnew-comparison-carousel').children().length > 0) {
                $('.findnew-comparison-carousel').slick({
                    slidesToShow: 3,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,
                    cssEase: 'ease',
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
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('ev_car_comparison', 'ev_car_comparison');
