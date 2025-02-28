<?php
// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/css/car-carousel.css';

function enqueue_ev_carousel_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);

    wp_enqueue_style(
        'cate-news-style',
        get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/car-carousel.css',
        array(),
        '1.0',
        'all'
    );
}

add_action('wp_enqueue_scripts', 'enqueue_ev_carousel_css');

function initialize_slick_slider_car_carousel()
{
?>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            $('.top10-sedan-cars-list').slick({
                slidesToShow: 4, // Shows 4 cars initially
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false, // Prevents looping
                cssEase: 'ease', // Smooth scrolling
                responsive: [{
                        breakpoint: 1030,
                        settings: {
                            slidesToShow: 3,
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
        });
    </script>
<?php
}
add_action('wp_footer', 'initialize_slick_slider_car_carousel');

function car_carousel($cars)
{
    ob_start();
?>
    <?php if (!empty($cars)) : ?>
        <div class="top10-sedan-cars-carousel">
            <ul class="top10-sedan-cars-list">
                <?php foreach ($cars as $car) : ?>
                    <?php
                    // Set up post data for the current car listing
                    // setup_postdata($car_post);

                    // Get car title, thumbnail GUID, and make terms
                    // $car_title = get_the_title($car_id); // Get the title for the listing

                    // // Get the post thumbnail ID and the corresponding guid (URL)
                    // $post_thumbnail_id = get_post_thumbnail_id($car_id);
                    // $thumbnail_post = get_post($post_thumbnail_id);
                    // $thumbnail_url = $thumbnail_post ? $thumbnail_post->guid : ''; // Use the guid for the image URL

                    // // Get the car makes
                    // $listing_makes = wp_get_post_terms($car_id, 'listing_make');
                    // $make_names = wp_list_pluck($listing_makes, 'name');

                    // $price_range = get_price_range($car_id);
                    ?>
                    <li class="car-sedan-item">
                        <div class="car-thumbnail">
                            <?php if ($car['thumbnail_url']) : ?>
                                <img src="<?php echo esc_attr($car['thumbnail_url']); ?>" alt="<?php echo esc_attr($car['post_title']); ?>">
                            <?php else : ?>
                                <img alt="Proton Saga" title="Proton Saga" src="https://images.wapcar.my/file1/88aaa06bdb554fb18c5b0e73651997ab_606x402.jpg">
                            <?php endif; ?>
                        </div>

                        <div class="car-content">
                            <div class="car-info">
                                <p class="car-make"><?php echo esc_html(implode(', ', $car['make_names'])); ?></p>
                            </div>
                            <a href="<?php echo get_permalink($car['id']); ?>" class="car-title"><?php echo esc_html($car['post_title']); ?></a>
                            <span class="price"><?php echo esc_html($car['price_range']); ?></span>

                            <a href="<?php echo get_permalink($car['id']); ?>" class="view-model-button"> ดูรุ่นรถ </a>
                        </div>
                    </li>
                <?php endforeach; ?>
            </ul>
        </div>
    <?php else : ?>
        <p>No cars available.</p>
<?php endif;

    wp_reset_postdata();
    return ob_get_clean();
}

// function get_price_range($car_id)
// {
//     $variants_query = new WP_Query([
//         'post_type' => 'variant',
//         'posts_per_page' => -1,
//         'meta_query' => [
//             [
//                 'key' => 'model',
//                 'value' => $car_id,
//                 'compare' => 'LIKE',
//             ],
//         ],
//     ]);

//     $lowest_price = null;
//     $highest_price = null;
//     if ($variants_query->have_posts()) {
//         while ($variants_query->have_posts()) {
//             $variants_query->the_post();
//             $price = get_post_meta(get_the_ID(), 'retail_price', true);
//             if ($price) {
//                 $price = (float)$price; // Ensure it's a float for comparison
//                 if (is_null($lowest_price) || $price < $lowest_price) {
//                     $lowest_price = $price;
//                 }
//                 if (is_null($highest_price) || $price > $highest_price) {
//                     $highest_price = $price;
//                 }
//             }
//         }
//         wp_reset_postdata();
//     }

//     // Prepare the price range text
//     if (!is_null($lowest_price) && !is_null($highest_price)) {
//         if ($lowest_price === $highest_price) {
//             $price_range = 'RM ' . number_format($lowest_price);
//         } else {
//             $price_range = 'RM ' . number_format($lowest_price) . ' - RM ' . number_format($highest_price);
//         }
//     } else {
//         $price_range = 'N/A';
//     }

//     return $price_range;
// }
?>