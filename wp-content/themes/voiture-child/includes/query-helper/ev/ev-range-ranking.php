<?php

function fetch_ev_range_ranking_data_from_db()
{
    $args = array(
        'post_type'      => 'listing',
        'posts_per_page' => -1,
        'meta_query'     => array(
            array(
                'key'   => 'is_ev',
                'value' => '1',
                'compare' => '='
            )
        )
    );

    $ev_car_models = new WP_Query($args);

    $ev_range_data = array();
    $post_ids = wp_list_pluck($ev_car_models->posts, 'ID');
    $thumbnail_urls = get_post_thumbnail_urls($post_ids);
    foreach ($ev_car_models->posts as $ev_car_model) {
        $serialized_value = 's:' . strlen((string)$ev_car_model->ID) . ':"' . $ev_car_model->ID . '";';
        $variant_args = array(
            'post_type'      => 'variant',
            'posts_per_page' => -1,
            'meta_query'     => array(
                array(
                    'key'   => 'model',
                    'value' => $serialized_value,
                    'compare' => 'LIKE',
                )
            ),
        );

        $variants = new WP_Query($variant_args);

        $min_ev_range = $max_ev_range = $ev_range = 0;

        foreach ($variants->posts as $variant) {
            $ev_range = get_field('ev_range', $variant->ID);
            $min_ev_range = $ev_range < $min_ev_range ? $ev_range : $min_ev_range;
            $max_ev_range = $ev_range > $max_ev_range ? $ev_range : $max_ev_range;
        }

        $thumbnail_url = $thumbnail_urls[$ev_car_model->ID] ?? '';

        $ev_range_data[] = array(
            'min_ev_range' => $min_ev_range,
            'max_ev_range' => $max_ev_range,
            'model_id'     => $ev_car_model->ID,
            'title'        => $ev_car_model->post_title,
            'thumbnail'    => $thumbnail_url,
        );
    }

    // sort by highest EV range
    usort($ev_range_data, function ($a, $b) {
        return $b['max_ev_range'] - $a['max_ev_range'];
    });

    // limit to top 5
    $ev_range_data = array_slice($ev_range_data, 0, 5);

    return $ev_range_data;
}
