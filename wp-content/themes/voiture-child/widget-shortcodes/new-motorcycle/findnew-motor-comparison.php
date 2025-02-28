<?php

function enqueue_find_new_motor_comparison_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);

    wp_enqueue_style('find-new-motor-comparison-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-motorcycle/css/find-new-cars-comparison.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_find_new_cars_comparison_css');

function initialize_findnew_motor_slider()
{
?>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            var itemCount = $('.findnew-comparison-carousel .findnew-comparison-item').length;
            var slidesToShow = Math.min(itemCount, 3); // Show up to 3 items

            if (itemCount > 0) {
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
}
add_action('wp_footer', 'initialize_findnew_motor_slider');


function findnew_bike_comparison($atts)
{
    enqueue_find_new_motor_comparison_css();

    $atts = shortcode_atts(array(
        'brand_id' => '',
        'post_includes' => '',
    ), $atts);

    $brand_id = (int) $atts['brand_id'];

    ob_start();
    $car_comparison_data = get_motor_comparison_data($brand_id);

    $posts = $car_comparison_data['posts'];
    $brand_name = $car_comparison_data['brand_name'];
    $num_posts = count($posts);

    if ($num_posts < 2) {
        return;
    }

?>
    <h2 class="wa-title-text find-new-cars-compare-title">เปรียบเทียบรถมอเตอร์ไซค์ <?php echo esc_html($brand_name); ?></h2>
    <div class="findnew-comparison-wrapper">
        <div class="findnew-comparison-carousel">
            <?php
            for ($i = 0; $i < $num_posts; $i += 2) :
                if (isset($posts[$i]) && isset($posts[$i + 1])) :
                    $listing1 = $posts[$i];
                    $listing2 = $posts[$i + 1];

                    // For Listing 1
                    $listing1_title = $listing1->post_title;
                    $listing1_price = $listing1->price_range;
                    $listing1_url = $listing1->permalink;
                    $listing1_slug = $listing1->post_name;
                    $listing1_image_guid = $listing1->thumbnail_url;

                    // $post_thumbnail_id1 = get_post_thumbnail_id($listing1_id);
                    // $thumbnail_post1 = get_post($post_thumbnail_id1);
                    // $listing1_image_guid = $thumbnail_post1 ? $thumbnail_post1->guid : '';

                    // For Listing 2
                    $listing2_title = $listing2->post_title;
                    $listing2_price = $listing2->price_range;
                    $listing2_url = $listing2->permalink;
                    $listing2_slug = $listing2->post_name;
                    $listing2_image_guid = $listing2->thumbnail_url;

                    // $post_thumbnail_id2 = get_post_thumbnail_id($listing2_id);
                    // $thumbnail_post2 = get_post($post_thumbnail_id2);
                    // $listing2_image_guid = $thumbnail_post2 ? $thumbnail_post2->guid : '';

                    $comparison_slug = $listing1_slug . '-vs-' . $listing2_slug;
            ?>
                    <div class="findnew-comparison-item">
                        <div class="car-comparison">
                            <a href="<?php echo $listing1_url; ?>" class="findnew-compare-card">
                                <div class="findnew-car-image-container">
                                    <img src="<?php echo esc_url($listing1_image_guid); ?>" alt="<?php echo esc_attr($listing1_title); ?>">
                                </div>
                                <div class="findnew-car-name"><?php echo esc_html($listing1_title); ?></div>
                                <div class="findnew-car-price"><?php echo esc_html($listing1_price); ?></div>
                            </a>
                            <div class="findnew-vs-container">
                                <span class="findnew-vs-tag">VS</span>
                            </div>
                            <a href="<?php echo $listing2_url; ?>" class="findnew-compare-card">
                                <div class="findnew-car-image-container">
                                    <img src="<?php echo esc_url($listing2_image_guid); ?>" alt="<?php echo esc_attr($listing2_title); ?>">
                                </div>
                                <div class="findnew-car-name"><?php echo esc_html($listing2_title); ?></div>
                                <div class="findnew-car-price"><?php echo esc_html($listing2_price); ?></div>
                            </a>
                        </div>
                        <a href="<?php echo home_url('/compare-motorcycles/') . $comparison_slug; ?>" class="findnew-compare-button">
                            <?php
                            // Split titles by spaces
                            $listing1_parts = explode(' ', $listing1_title);
                            $listing2_parts = explode(' ', $listing2_title);

                            // Remove the first part (the first name) and join the rest
                            $listing1_remaining = implode(' ', array_slice($listing1_parts, 1));
                            $listing2_remaining = implode(' ', array_slice($listing2_parts, 1));

                            // Output the comparison
                            echo esc_html($listing1_remaining); ?> vs <?php echo esc_html($listing2_remaining);
                                                                        ?>
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
                        },
                    ]
                });
            }
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('findnew_bike_comparison', 'findnew_bike_comparison');
