<?php
function fetch_top_10_sedan_data_from_db($args = array())
{

    $top_car_model_data = get_option('top_car_models', []);
    $sedan_cars_ids = $top_car_model_data['sedan_cars']['car_models'] ?? [];

    $top_car_model_ids = array_column($sedan_cars_ids, 'id');

    if (!empty($top_car_model_ids)) {
        // Query the listings for the retrieved sedan car model IDs
        $posts = get_posts(array(
            'post_type' => 'listing',
            'posts_per_page' => 10,
            'post__in' => array_values($top_car_model_ids),
            'orderby' => 'post__in',
        ));

        // Preload all meta data for queried posts
        $post_ids = wp_list_pluck($posts, 'ID');
        $all_meta = get_post_meta_with_thumbnail_guid($post_ids);

        $sedan_cars = [];
        foreach ($posts as $post) {
            $post_meta = $all_meta[$post->ID] ?? [];
            $car_title = $post->post_title;
            $thumbnail_url = $post_meta['_thumbnail_guid'] ? $post_meta['_thumbnail_guid'] : '';
            $make_term_id = $post_meta['_listing_make'][0] ?? '';
            $make_name = $make_term_id ? get_term($make_term_id)->name : '';
            $price_range = get_price_range_of_listing($post->ID);

            $listing_states = [
                'On Sale' => ['label' => 'Hot', 'color' => '#F53030'],
                'Not On Sale' => ['label' => 'Not On Sale', 'color' => '#AAAAAA'],
                'Upcoming' => ['label' => 'Upcoming', 'color' => '#32D0C6']
            ];

            $listing_state = $post_meta['listing-state'][0] ?? 'Not On Sale';
            $state = $listing_states[$listing_state] ?? ['label' => 'Not On Sale', 'color' => '#AAAAAA'];

            $sedan_cars[] = [
                'id' => $post->ID,
                'title' => $car_title,
                'thumbnail_url' => $thumbnail_url,
                'make_name' => $make_name,
                'price_range' => $price_range,
                'state' => $state,
            ];
        }

        return $sedan_cars;
    }

    return [];
}
