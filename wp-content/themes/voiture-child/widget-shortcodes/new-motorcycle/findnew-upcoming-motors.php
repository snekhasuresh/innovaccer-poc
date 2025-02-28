<?php
function enqueue_find_new_upcoming_bike_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);
    wp_enqueue_style('find-new-upcoming-bike-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-motorcycle/css/find-new-upcoming-cars.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_find_new_upcoming_cars_css');

function initialize_slick_slider_upcoming_motor()
{
?>
    <script>
        jQuery(document).ready(function($) {
            $('.upcoming-cars-list').slick({
                slidesToShow: 3, // Shows 4 cars initially
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false, // Prevents looping
                cssEase: 'ease', // Smooth scrolling
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
        });
    </script>
    <?php
}
add_action('wp_footer', 'initialize_slick_slider_upcoming_motor');

// Shortcode for Top 10 Sedan Cars
function upcoming_bikes_shortcode($atts)
{
    enqueue_find_new_upcoming_bike_css();

    $atts = extract(shortcode_atts(array(
        'count' => 10,
        'posts_include' => '',
    ), $atts));

    $args = array(
        'post_type'      => 'upcoming-car',
        'posts_per_page' => 10, // Get all upcoming car posts
    );


    if (!empty($posts_include)) {
        print_r('posts_include: ');
        print_r($posts_include);
        print_r('<br/>');
        // apply post parent filter
        // $args['post_parent__in'] = explode(',', $posts_include);
    }

    $upcoming_cars = get_posts($args);

    if (!empty($upcoming_cars)) { // Check if there are any upcoming cars
        ob_start(); // Start output buffering
    ?>
        <div class="upcoming-cars-carousel">
            <ul class="upcoming-cars-list">
                <?php foreach ($upcoming_cars as $car_post) : // Loop through each upcoming car post 
                ?>
                    <?php
                    // Get the post ID of the car
                    $car_id = $car_post->ID;

                    // Retrieve meta fields using get_post_meta()
                    $car_title = get_post_meta($car_id, 'car_title', true);
                    $brand_model = get_post_meta($car_id, 'brand-model', true);
                    $brand_model_display = !empty($brand_model) && is_array($brand_model) ? implode(', ', $brand_model) : 'No Brand Model';
                    $brand_model_name = get_post($brand_model_display);
                    // print_r($brand_model_display);
                    $brand_make_id = get_post_meta($brand_model_display, '_listing_make', true);
                    // print_r($brand_make_id);
                    $term = get_term($brand_make_id, 'listing_make'); // Replace 'listing_make' with your actual taxonomy slug

                    if (!is_wp_error($term) && !empty($term)) {
                        $make_name = $term->name; // Get the term name
                    } else {
                        $make_name = 'No Brand Model'; // Fallback if there is an error or term doesn't exist
                    }
                    $price_from = get_post_meta($car_id, 'price_from', true);
                    $price_to = get_post_meta($car_id, 'price_to', true);
                    $thumbnail_url = get_post_meta($car_id, 'list_picture', true); // Assuming it's stored as image URL or attachment ID
                    $thumbnail_post = get_post($thumbnail_url);
                    $guid = $thumbnail_post->guid; // Get the GUID of the thumbnail


                    ?>

                    <li class="car-sedan-item">
                        <div class="car-thumbnail">
                            <?php if ($guid) : // Use $guid for the image 
                            ?>
                                <img src="<?php echo esc_url($guid); ?>" alt="<?php echo esc_attr($car_title); ?>">
                            <?php else : ?>
                                < <?php endif; ?>
                                    </div>

                                    <div class="car-content">
                                        <div class="car-info">
                                            <p class="car-sub"><?php echo esc_html($make_name); ?></p>

                                            <p class="car-brand-model"><?php echo esc_html($brand_model_name->post_title); ?></p>
                                        </div>
                                        <a href="#" class="car-title"><?php echo esc_html($car_title); ?></a>
                                        <span class="price">
                                            <?php
                                            if ($price_from && $price_to) {
                                                echo 'RM ' . number_format($price_from) . ' - RM ' . number_format($price_to);
                                            } elseif ($price_from) {
                                                echo 'RM ' . number_format($price_from);
                                            } else {
                                                echo 'Price not available';
                                            }
                                            ?>
                                        </span>

                                    </div>
                    </li>
                <?php endforeach; ?>
            </ul>
        </div>
<?php
        return ob_get_clean(); // Return the buffered content
    }
    return '<p>No upcoming cars found.</p>'; // Message if no cars found
}
add_shortcode('upcoming_bikes', 'upcoming_bikes_shortcode');
