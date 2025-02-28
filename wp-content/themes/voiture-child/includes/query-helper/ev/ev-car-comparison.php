<?php

function fetch_ev_car_comparison_data_from_db()
{
    $args = array(
        'post_type'      => 'listing',
        'posts_per_page' => 10,
        'meta_query' => array(
            array(
                'key' => 'is_ev',
                'value' => '1',
                'compare' => '=='
            )
        ),
        'orderby' => 'ID',
        'order' => 'ASC'
    );

    $comparison_query = new WP_Query($args);

    if ($comparison_query->have_posts()) {
        $post_ids = wp_list_pluck($comparison_query->posts, 'ID');
        $thumbnail_urls = get_post_thumbnail_urls($post_ids);
        // thumbnail, price_range, permalink
        foreach ($comparison_query->posts as $post) {
            $price_range = get_price_range_of_listing($post->ID);
            $permalink = get_permalink($post->ID);
            $thumbnail_url = isset($thumbnail_urls[$post->ID]) ? $thumbnail_urls[$post->ID] : '';

            $post->thumbnail_url = $thumbnail_url;
            $post->price_range = $price_range;
            $post->permalink = $permalink;
        }

        return $comparison_query->posts;
    }

    return [];
}
