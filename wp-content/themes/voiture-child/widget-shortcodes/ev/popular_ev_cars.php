<?php
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/car-carousel.php';

function enqueue_ev_popular_css()
{
    wp_enqueue_style('ev-popular-style',  get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/car-carousel.css',);
}
add_action('wp_enqueue_scripts', 'enqueue_ev_popular_css');

function popular_ev_cars_shortcode($atts)
{
    $popular_cars = get_popular_ev_cars_data();

    ob_start();

?>
    <div class="popular-ev-head">
        <h2 class="wa-title-text">รถ EV ยอดนิยม</h2>
    </div>

    <?php
    echo car_carousel($popular_cars);
    ?>
    <div class="view-more-container">
        <a href="<?php home_url('/new-cars/best-ev'); ?>" class="view-more-button">
            <?php echo esc_html__('ดูเพิ่มเติม', 'voiture'); ?> <span>&#8250;</span>
        </a>
    </div>
<?php

    wp_reset_postdata();

    return ob_get_clean();
}

add_shortcode('popular_ev_cars', 'popular_ev_cars_shortcode');
