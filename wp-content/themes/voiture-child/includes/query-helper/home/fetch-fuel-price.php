<?php

function fetch_fuel_price_data_from_db() {
    $petrol_term = get_term_by('slug', 'petrol', 'fuel-type');
    $diesel_term = get_term_by('slug', 'diesel', 'fuel-type');

    // Safely handle missing terms
    $petrol_term_id = $petrol_term ? $petrol_term->term_id : null;
    $diesel_term_id = $diesel_term ? $diesel_term->term_id : null;
    $petrol_prices = [];
    $diesel_prices = [];
    $latest_update_time = '';

    $oil_posts = get_posts([
        'post_type' => 'oil',
        'numberposts' => -1,
    ]);

    // all oil pos ids
    $oil_post_ids = wp_list_pluck($oil_posts, 'ID');
    $all_meta_data = get_post_meta_for_posts($oil_post_ids);
    
    foreach ($oil_posts as $oil_post) {
        // post meta
        $post_meta = $all_meta_data[$oil_post->ID] ?? [];
        $fuel_type = $post_meta['fuel_type'][0];
        $oil_price = $post_meta['oil_price'][0];
        $change_time = $post_meta['change_time'][0];

        if ($change_time && strtotime($change_time) > strtotime($latest_update_time)) {
            $latest_update_time = $change_time;
        }

        // Store prices based on fuel type, comparing to valid term IDs
        if ($fuel_type == $petrol_term_id) {
            $petrol_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
            ];
        } elseif ($fuel_type == $diesel_term_id) {
            $diesel_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
            ];
        }
    }
    $formatted_update_time = !empty($latest_update_time) ? date('F j, Y', strtotime($latest_update_time)) : 'No data';

    return [
        'petrol_prices' => $petrol_prices,
        'diesel_prices' => $diesel_prices,
        'latest_update_time' => $formatted_update_time,
    ];
}