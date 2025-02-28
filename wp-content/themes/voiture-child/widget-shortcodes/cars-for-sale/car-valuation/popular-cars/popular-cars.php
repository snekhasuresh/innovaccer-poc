<?php
function enqueue_popular_car_css()
{
    wp_enqueue_style('popular-cars-style',  get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/car-carousel.css',);
}
add_action('wp_enqueue_scripts', 'enqueue_popular_car_css');

function popular_cars_shortcode($atts)
{
    $cache_key = 'popular_cars_car_valuation';
    $popular_cars = get_transient($cache_key);
    if (false === $popular_cars) {
        $car_categories = get_option('recommended_car_models', []);

        $popular_car_model_ids = [];
        $popular_car_models = [];
        foreach ($car_categories as $key => $data) {
            // get only popular category cars
            if ($key == 'Popular') {
                foreach ($data['car_models'] as $car_model) {
                    $popular_car_models[] = ['id' => $car_model['id'], 'sort' => $car_model['sort']];
                }
            }
        }

        usort($popular_car_models, function ($a, $b) {
            return $a['sort'] - $b['sort'];
        });

        foreach ($popular_car_models as $popular_car_model) {
            $popular_car_model_ids[] = $popular_car_model['id'];
        }

        $args = array(
            'post_type'      => 'listing',
            'posts_per_page' => 10,
            'post__in' => $popular_car_model_ids,
            'fields'         => 'ids',
        );

        $popular_cars = get_posts($args);
        set_transient($cache_key, $popular_cars, HOUR_IN_SECONDS);
    }
    ob_start();

?>
    <h2 class="wa-title-text">Popular Cars</h2>
    <?php
    echo car_carousel($popular_cars);
    ?>
    <div class="view-more-container">
        <a href="<?php home_url('/new-cars/best-ev'); ?>" class="view-more-button">
            <?php echo esc_html__('View More', 'voiture'); ?> <span>&#8250;</span>
        </a>
    </div>
<?php

    wp_reset_postdata();

    return ob_get_clean();
}

add_shortcode('popular_cars', 'popular_cars_shortcode');
