<?php

function fetch_petrol_and_diesel_data_from_db()
{
    // Get all oil posts and their meta data
    $oil_posts = get_posts([
        'post_type' => 'oil',
        'numberposts' => -1,
    ]);

    // Get terms for petrol and diesel
    $petrol_term = get_term_by('slug', 'petrol', 'fuel-type');
    $diesel_term = get_term_by('slug', 'diesel', 'fuel-type');

    // Seggregate petrol prices and diesel prices
    $petrol_prices = [];
    $diesel_prices = [];

    // get post ids of oil posts
    $post_ids = array_map(function ($oil_post) {
        return $oil_post->ID;
    }, $oil_posts);
    $required_meta_keys = ['fuel_type', 'oil_price'];
    $post_meta = get_selected_meta_data_for_posts($post_ids, $required_meta_keys);
    foreach ($oil_posts as $oil_post) {
        $fuel_type = $post_meta[$oil_post->ID]['fuel_type'][0];
        $oil_price = $post_meta[$oil_post->ID]['oil_price'][0];

        if ($fuel_type == $petrol_term->term_id) {
            $petrol_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
            ];
        } elseif ($fuel_type == $diesel_term->term_id) {
            $diesel_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
            ];
        }
    }

    return [
        'petrol' => $petrol_prices,
        'diesel' => $diesel_prices,
    ];
}

// Function to get historical prices
function fetch_historical_oil_price_data_from_db($args)
{
    global $wpdb;
    $table_name = $wpdb->prefix . 'historical_oil_price';

    $oil_name = $args['oil_name'];
    $start_date = $args['start_date'];
    $end_date = $args['end_date'];

    $where = array();
    $values = array();

    if ($oil_name) {
        $where[] = 'oil_name = %s';
        $values[] = $oil_name;
    }

    if ($start_date) {
        $where[] = 'start_date >= %s';
        $values[] = $start_date;
    }

    if ($end_date) {
        $where[] = 'end_date <= %s';
        $values[] = $end_date;
    }

    $where_clause = !empty($where) ? 'WHERE ' . implode(' AND ', $where) : '';

    $query = $wpdb->prepare(
        "SELECT * FROM $table_name $where_clause ORDER BY created_at DESC",
        $values
    );

    $results = $wpdb->get_results($query);
    return $results;
}

