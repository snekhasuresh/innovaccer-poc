<?php
// import car-comparison.css
function enqueue_motor_comparison_css()
{
    wp_enqueue_style('car_comparison-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/overview/motor/css/car-comparison.css', array(), '1.0', 'all');
}

function single_listing_motor_comparison()
{
    enqueue_motor_comparison_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    if (empty($global_listing_post_data) || !array($global_listing_post_data)) {
        return;
    }

    $global_listing_post = $global_listing_post_data['post'];
    $post_title = $global_listing_post->post_title;
    $post_id = $global_listing_post->ID;
    $post_name = $global_listing_post->post_name;
    $post_image = $global_listing_post_data['thumbnail'];
    $post_permalink = get_permalink($post_id);
    $post_price = $global_listing_post_data['min_price'];
    if ($post_price) {
        $post_price = format_price_vietnam($post_price);
    } else {
        $post_price = 'Đang cập nhật';
    }

    // WP Query to retrieve other listings for comparison
    $args = array(
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 10, // Adjust number as needed
        'post__not_in' => array($post_id), // Exclude current listing
    );

    $comparison_query = new WP_Query($args);

    if (!$comparison_query->have_posts()) {
        return;
    }

    $comparison_car_ids = wp_list_pluck($comparison_query->posts, 'ID');
    $comparison_car_meta = get_post_meta_with_thumbnail_guid($comparison_car_ids);
?>
    <div class="individual-comparison-wrapper">
        <h2 class='wa-title-text'><span class="icon"></span>So sánh <?php echo $post_title; ?></h2>
        <div class="individual-comparison-carousel">

            <?php foreach ($comparison_query->posts as $comparison_post) : ?>
                <div class="individual-comparison-item">
                    <div class="car-comparison">
                        <!-- Left Side (Dynamic Data from Current Listing) -->
                        <div class="compare-card">
                            <div class="car-image-container">
                                <img src="<?php echo esc_url($post_image); ?>" alt="<?php echo esc_attr($post_title); ?>">
                            </div>
                            <a href="<?php echo $post_permalink; ?>" class="car-name"><?php echo esc_html($post_title); ?></a>
                            <div class="car-price"><?php echo $post_price ?></div>
                        </div>
                        <div class="vs-container">
                            <span class="vs-tag">VS</span>
                        </div>

                        <!-- Right Side (Dynamic Data from WP_Query) -->
                        <div class="compare-card">
                            <div class="car-image-container">
                                <?php
                                $comparison_post_id = $comparison_post->ID;
                                $comparison_post_meta = $comparison_car_meta[$comparison_post->ID];
                                $comparison_image_guid = $comparison_post_meta['_thumbnail_guid'];
                                $comparison_post_title = $comparison_post->post_title;
                                $comparison_post_slug = $comparison_post->post_name;
                                $comparison_post_permalink = get_permalink($comparison_post_id);
                                $comparison_post_price = explode('-', get_price_range_of_listing($comparison_post_id))[0];
                                $comparison_slug = $post_name . '-vs-' . $comparison_post_slug;
                                ?>
                                <img src="<?php echo esc_url($comparison_image_guid); ?>" alt="<?php echo $comparison_post_title; ?>">
                            </div>
                            <a href="<?php echo $comparison_post_permalink; ?>" class="car-name"><?php echo $comparison_post_title; ?></a>
                            <div class="car-price">
                                <?php echo $comparison_post_price; ?>
                            </div>
                        </div>

                    </div>
                    <a href="<?php echo home_url('/compare-motorcycles/') . $comparison_slug; ?>" class="compare-button">
                        <?php echo esc_html($post_title); ?> vs <?php echo $comparison_post_title; ?>
                    </a>
                </div>
            <?php endforeach; ?>
            <?php wp_reset_postdata(); ?>
        </div>
    </div>

    <script type="text/javascript">
        jQuery(document).ready(function($) {
            $('.individual-comparison-carousel').slick({
                slidesToShow: 3,
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false, // Prevents looping
                cssEase: 'ease', // Smooth scrolling
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
        });
    </script>
<?php

}
add_shortcode('single_listing_motor_comparison', 'single_listing_motor_comparison');
