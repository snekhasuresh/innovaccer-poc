<?php
function initialize_findnew_slider2()
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
                            breakpoint: 600,
                            settings: {
                                slidesToShow: slidesToShow,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 480,
                            settings: {
                                slidesToShow: slidesToShow,
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
add_action('wp_footer', 'initialize_findnew_slider2');
function individual_listing_car_comparison($atts)
{

    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    // get listing post by post name
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));
    ob_start();

    $args = array(
        'post_type' => 'listing',
        'posts_per_page' => 10, // Adjust number as needed
        'post__not_in' => array($listing_post[0]->ID), // Exclude current listing
    );

    $comparison_query = new WP_Query($args);

?>
    <div class="findnew-comparison-wrapper">
        <div class="findnew-comparison-carousel">
            <?php if ($comparison_query->have_posts()) : ?>
                <?php
                $posts = $comparison_query->posts;
                $num_posts = count($posts);

                for ($i = 0; $i < $num_posts; $i += 1) :
                    if (isset($posts[$i]) && isset($posts[$i + 1])) :
                        $listing1 = $listing_post[0];
                        $listing2 = $posts[$i];

                        // For Listing 1
                        $listing1_id = $listing1->ID;
                        $listing1_title = get_the_title($listing1_id);
                        $listing1_price = get_post_meta($listing1_id, '_listing_price', true);
                        $post_thumbnail_id1 = get_post_thumbnail_id($listing1_id);
                        $thumbnail_post1 = get_post($post_thumbnail_id1);
                        $listing1_image_guid = $thumbnail_post1 ? $thumbnail_post1->guid : '';

                        // For Listing 2
                        $listing2_id = $listing2->ID;
                        $listing2_title = get_the_title($listing2_id);
                        $listing2_price = get_post_meta($listing2_id, '_listing_price', true);
                        $post_thumbnail_id2 = get_post_thumbnail_id($listing2_id);
                        $thumbnail_post2 = get_post($post_thumbnail_id2);
                        $listing2_image_guid = $thumbnail_post2 ? $thumbnail_post2->guid : '';
                ?>
                        <div class="findnew-comparison-item">
                            <div class="car-comparison">
                                <div class="findnew-compare-card">
                                    <div class="findnew-car-image-container">
                                        <img src="<?php echo esc_url($listing1_image_guid); ?>" alt="<?php echo esc_attr($listing1_title); ?>">
                                    </div>
                                    <div class="findnew-car-name"><?php echo esc_html($listing1_title); ?></div>
                                    <div class="findnew-car-price">RM <?php echo esc_html($listing1_price); ?></div>
                                </div>
                                <div class="findnew-vs-container">
                                    <span class="findnew-vs-tag">VS</span>
                                </div>
                                <div class="findnew-compare-card">
                                    <div class="findnew-car-image-container">
                                        <img src="<?php echo esc_url($listing2_image_guid); ?>" alt="<?php echo esc_attr($listing2_title); ?>">
                                    </div>
                                    <div class="findnew-car-name"><?php echo esc_html($listing2_title); ?></div>
                                    <div class="findnew-car-price">RM <?php echo esc_html($listing2_price); ?></div>
                                </div>
                            </div>
                            <a href="#" class="findnew-compare-button">
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
            <?php else : ?>
                <p>No comparison listings found.</p>
            <?php endif; ?>
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
                            breakpoint: 1024,
                            settings: {
                                slidesToShow: 3,
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
        });
    </script>

    <style>
        .findnew-comparison-wrapper h2::before,
        .pc-title::before,
        .wa-title::before {
            content: " ";
            display: block;
            width: 14px;
            height: 26px;
            transform: skewX(345deg);
            background-image: linear-gradient(90deg, #32D0C6 0%, #32D0C6 64%, #0B0E52 64%, #0B0E52 100%);

        }

        .findnew-comparison-wrapper {
            min-height: 215px;
        }

        .findnew-comparison-carousel {
            display: flex;
            justify-content: space-between;
            gap: 20px;
        }

        .slick-track {
            display: flex !important;
            gap: 20px;
            min-width: 1200px !important;
        }

        .slick-list .draggable {
            margin-left: -20px;
        }

        /* Slick Previous/Next button styles */
        .slick-prev,
        .slick-next {
            background-color: #ffffff !important;
            /* Default white background */
            border-radius: 50%;
            width: 50px;
            height: 50px;
            z-index: 10;
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            display: flex;
            justify-content: center;
            align-items: center;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
            transition: background-color 0.3s ease, color 0.3s ease;
            border: none;
        }

        .slick-prev {
            margin-left: 7px;
        }

        .slick-next {
            margin-right: 10px;
        }

        .slick-prev:before,
        .slick-next:before {
            font-size: 20px;
            color: black !important;
            /* Black arrow */
        }

        /* Hover state */
        .slick-prev:hover,
        .slick-next:hover {
            background-color: white !important;
            /* Change to yellow on hover */
            color: white !important;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;

        }

        /* Focus state */
        .slick-prev:focus,
        .slick-next:focus {
            background-color: white !important;
            /* Keep background yellow on focus */
            color: white !important;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;

            outline: none;
        }

        /* Active state when button is clicked */
        .slick-prev:active,
        .slick-next:active {
            background-color: white !important;
            /* Keep background yellow on click */
            color: white !important;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;

            outline: none;
        }

        /* Focus-visible state for keyboard users */
        .slick-prev:focus-visible,
        .slick-next:focus-visible {
            background-color: white !important;
            color: white !important;
            outline: none;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15);

        }

        /* Prevent disappearing of background by using more specific selectors */
        button.slick-prev,
        button.slick-next,
        div.slick-prev,
        div.slick-next {
            background-color: white !important;
            color: white !important;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15);
        }

        /* Slick Dots customization */
        .slick-prev {
            left: -25px;
        }

        /* Position for the Next arrow */
        .slick-next {
            right: -25px;
        }

        .findnew-comparison-item {
            flex: 0 0 300;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            transition: transform 0.1s ease;
            position: relative;
            min-width: 200px;
            min-height: 200px;
        }

        .findnew-comparison-item:hover {
            transform: translateY(-5px);
        }

        .car-comparison {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 15px;
        }

        .findnew-car-image {
            flex: 0 0 45%;
            text-align: center;

        }

        .findnew-car-image img {
            max-width: 100%;
            height: auto;
            border-radius: 4px;
        }

        .findnew-car-image h3 {
            font-size: 16px;
            color: #333;
            margin: 10px 0;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .findnew-car-image .price {
            font-size: 14px;
            font-weight: bold;
            color: #576B95;

        }

        /* Card container */
        .findnew-compare-card {
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            width: 156px;
            overflow: hidden;
            box-sizing: border-box;
            transition: transform 0.3s ease, box-shadow 0.3s ease, border 0.3s ease;
        }

        .findnew-car-image-container {
            width: 115px;
            height: 60px;
            text-align: center;
            overflow: hidden;
            display: flex;
            justify-content: center;
            align-items: center;
            border-radius: 4px;
            background-color: #ffff;
        }

        /* Ensure images fit the container without cropping */
        .findnew-car-image-container img {
            width: 110px;
            height: 74px;
            display: block;
        }

        /* Car name styling */
        .findnew-car-name {
            font-size: 12px;
            font-weight: bold;
            color: #333;
            text-align: start;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
            margin-top: 20px;
        }

        /* Pricing section */
        .findnew-car-price {
            font-size: 14px;
            color: #576B95;
            text-align: start;
            margin-top: -7px;
            font-weight: 700;
        }

        .findnew-vs-container {
            display: flex;
            align-items: center;
            justify-content: center;
            flex-direction: column;
            /* Stack vertically */
            position: relative;
            height: 120px;
            /* Adjust as needed */
        }

        /* Vertical line above the VS tag */
        .findnew-vs-container::before {
            content: '';
            width: 1px;
            height: 40px;
            /* Adjust the height of the top vertical line */
            background-color: #ddd;
            /* Divider color */
            position: absolute;
            top: 0;
        }

        /* Vertical line below the VS tag */
        .findnew-vs-container::after {
            content: '';
            width: 1px;
            height: 40px;
            /* Adjust the height of the bottom vertical line */
            background-color: #ddd;
            /* Divider color */
            position: absolute;
            bottom: 0;
        }

        /* VS tag styling */
        .findnew-vs-tag {
            background-color: #333;
            color: white;
            font-weight: bold;
            font-size: 12px;
            padding: 1px 4px;
            border-radius: 50%;
            z-index: 1;
            position: relative;
        }

        .findnew-comparison-item .findnew-compare-button {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            width: 100%;
            padding: 6px;
            background-color: white;
            color: black;
            border: 2px solid #32D0C6;
            border-radius: 4px;
            text-decoration: none;
            font-weight: bold;
            transition: background-color 0.3s, color 0.3s;
            display: inline-block;
            box-sizing: border-box;
        }

        .carousel-prev,
        .carousel-next {
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            background-color: #fff;
            border: 1px solid #ddd;
            border-radius: 50%;
            padding: 10px;
            font-size: 24px;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }

        .carousel-prev:hover,
        .carousel-next:hover {
            background-color: #ddd;
        }

        .carousel-prev {
            left: -30px;
        }

        .carousel-next {
            right: -30px;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('individual_listing_car_comparison', 'individual_listing_car_comparison');
