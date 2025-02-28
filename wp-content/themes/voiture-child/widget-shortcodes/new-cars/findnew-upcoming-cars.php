<?php

function enqueue_find_new_upcoming_cars_css()
{
    wp_enqueue_style('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css', [], '1.8.1');
    wp_enqueue_style('slick-theme', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css', [], '1.8.1');
    wp_enqueue_script('slick-carousel', 'https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js', ['jquery'], '1.8.1', true);

    wp_enqueue_style('find-new-upcoming-cars-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/find-new-upcoming-cars.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_find_new_upcoming_cars_css');

function initialize_slick_slider_upcoming_cars()
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
add_action('wp_footer', 'initialize_slick_slider_upcoming_cars');

// Shortcode for Top 10 Sedan Cars
function upcoming_cars_shortcode($atts)
{
    enqueue_find_new_upcoming_cars_css();

    $upcoming_cars_data = get_latest_cars_data();  

    if (empty($upcoming_cars_data)) {
        return;
    }

    ob_start(); // Start output buffering
?>
    <div class="upcoming-cars-carousel">
        <ul class="upcoming-cars-list">
            <?php foreach ($upcoming_cars_data as $upcoming_car) :

                $car_id = $upcoming_car['id'];
                $car_title = $upcoming_car['post_title'];
                $guid = $upcoming_car['thumbnail_url'];
                $price_range = $upcoming_car['price_range'];
                $post_name = $upcoming_car['post_name'];
                $listing_state = $upcoming_car['listing_state'];
                $permalink = $upcoming_car['permalink'];
                $listing_make = $upcoming_car['listing_make'];

            ?>
                <li class="car-sedan-item" style="width:320px !important;">
                    <div class="car-thumbnail">
                        <img src="<?php echo esc_url($guid); ?>" alt="<?php echo esc_attr($car_title); ?>">
                    </div>

                    <div class="car-content">
                        <div class="car-info">
                            <p class="car-sub"><span class="car-brand-icon">.</span><?php echo esc_html($listing_make); ?></p>
                        </div>
                        <a href="<?php echo $permalink; ?>" class="car-title"><?php echo esc_html($car_title); ?></a>
                        <span class="price">
                            <?php echo $price_range; ?>
                        </span>

                    </div>
                </li>
            <?php endforeach; ?>
        </ul>
    </div>
<?php
    return ob_get_clean(); // Return the buffered content
}
add_shortcode('upcoming_cars', 'upcoming_cars_shortcode');
