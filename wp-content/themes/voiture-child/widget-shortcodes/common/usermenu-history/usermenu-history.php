<?php
include_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/find-new-cars.php';

function usermenu_history()
{
    $top_car_model_ids = get_option('top_car_models', []);
    $args = array(
        'post_type' => 'listing',
        'posts_per_page' => -1,
        'post__in' => $top_car_model_ids,
        'orderby' => 'post__in',
    );

    $cache_key = 'popular_car_posts';
    $popular_car_posts = get_transient($cache_key);

    if ($popular_car_posts === false) {
        $popular_car_posts = get_posts($args);
        set_transient($cache_key, $popular_car_posts, HOUR_IN_SECONDS);
    }

    if (empty($popular_car_posts)) {
        return '';
    }
    ob_start();


    display_popular_car_posts($popular_car_posts);

    return ob_get_clean();
}

add_shortcode('usermenu_history', 'usermenu_history');
