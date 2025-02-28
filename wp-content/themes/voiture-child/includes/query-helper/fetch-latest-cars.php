<?php

function fetch_latest_cars_data_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;
    $posts_per_page = $args['posts_per_page'] ?? 10;
    $latest_cars = get_posts(array(
        'post_type'      => 'upcoming-car',
        'posts_per_page' => $posts_per_page,
        'meta_query'     => array(
            array(
                'key'     => 'time_to_launch',
                'compare' => 'EXISTS',
                'type'    => 'DATETIME',
            ),
        ),
        'orderby'  => array(
            'meta_value' => 'DESC',
            'ID'    => 'ASC',
        ),
        'meta_key' => 'time_to_launch',
        'meta_type' => 'DATETIME',
    ));

    $upcoming_car_ids = array();

    foreach ($latest_cars as $key => $latest_car) {
        $upcoming_car_ids[] = $latest_car->post_parent;
    }

    $latest_cars_response = format_car_response($upcoming_car_ids, $need_variant_info);

    return $latest_cars_response;
}
