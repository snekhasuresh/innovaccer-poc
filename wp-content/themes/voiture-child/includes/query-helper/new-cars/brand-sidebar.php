<?php

function fetch_brand_sidebar_data_from_db()
{
    $car_brands = get_terms([
        'taxonomy'   => 'listing_make',
        'hide_empty' => false,
        'parent'     => 0,
        'meta_query' => [
            'relation' => 'OR',
            ['key' => 'state', 'compare' => 'NOT EXISTS'],
            ['key' => 'state', 'value' => '1', 'compare' => '=']
        ],
        'orderby'    => 'name',
        'order'      => 'ASC',
    ]);

    if (empty($car_brands)) {
        return [];
    }
    $brands_by_letter = group_brands_by_letter($car_brands);

    return $brands_by_letter;
}

function group_brands_by_letter($brands)
{
    $brands_by_letter = [];
    $term_ids = wp_list_pluck($brands, 'term_id'); // Get all term IDs from brands

    // Fetch all meta values in bulk for the given term IDs
    $meta_values = get_term_meta_bulk($term_ids, ['state', 'sort', 'listing_make_image']); // Custom function

    foreach ($brands as $brand) {
        $state = $meta_values[$brand->term_id]['state'] ?? null;
        $logo = $meta_values[$brand->term_id]['listing_make_image'] ?? null;
        $brand->state = $state;
        $brand->logo = $logo;

        if ($state == 1) {
            $first_letter = strtoupper(substr($brand->name, 0, 1));
            $brands_by_letter[$first_letter][] = $brand;
        }
    }

    foreach ($brands_by_letter as $letter => &$brands) {
        usort($brands, function ($a, $b) use ($meta_values) {
            $sort_a = intval($meta_values[$a->term_id]['sort'] ?? 0);
            $sort_b = intval($meta_values[$b->term_id]['sort'] ?? 0);
            return $sort_a - $sort_b;
        });
    }

    return $brands_by_letter;
}

function get_term_meta_bulk($term_ids, $meta_keys)
{
    global $wpdb;

    if (empty($term_ids) || empty($meta_keys)) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($meta_keys), '%s'));
    $query = $wpdb->prepare(
        "SELECT term_id, meta_key, meta_value 
         FROM {$wpdb->termmeta} 
         WHERE term_id IN (" . implode(',', array_map('intval', $term_ids)) . ") 
         AND meta_key IN ($placeholders)",
        $meta_keys
    );

    $results = $wpdb->get_results($query);

    $meta_data = [];
    foreach ($results as $row) {
        if (!isset($meta_data[$row->term_id])) {
            $meta_data[$row->term_id] = [];
        }
        $meta_data[$row->term_id][$row->meta_key] = $row->meta_value;
    }

    return $meta_data;
}

function fetch_motor_brand_sidebar_data_from_db()
{
    $car_brands = get_terms([
        'taxonomy'   => 'motorcycle_make',
        'hide_empty' => false,
        'parent'     => 0,
        'meta_query' => [
            'relation' => 'OR',
            ['key' => 'state', 'compare' => 'NOT EXISTS'],
            ['key' => 'state', 'value' => '1', 'compare' => '=']
        ],
        'orderby'    => 'name',
        'order'      => 'ASC',
    ]);

    if (empty($car_brands)) {
        return [];
    }
    $brands_by_letter = group_motor_brands_by_letter($car_brands);

    return $brands_by_letter;
}

function group_motor_brands_by_letter($brands)
{
    $brands_by_letter = [];
    $term_ids = wp_list_pluck($brands, 'term_id'); // Get all term IDs from brands

    // Fetch all meta values in bulk for the given term IDs
    $meta_values = get_term_meta_bulk($term_ids, ['state', 'sort', 'motorcycle_make_image']); // Custom function

    foreach ($brands as $brand) {
        $state = $meta_values[$brand->term_id]['state'] ?? null;
        $logo = $meta_values[$brand->term_id]['motorcycle_make_image'] ?? null;
        $brand->state = $state;
        $brand->logo = $logo;

        if ($state == 1) {
            $first_letter = strtoupper(substr($brand->name, 0, 1));
            $brands_by_letter[$first_letter][] = $brand;
        }
    }

    foreach ($brands_by_letter as $letter => &$brands) {
        usort($brands, function ($a, $b) use ($meta_values) {
            $sort_a = intval($meta_values[$a->term_id]['sort'] ?? 0);
            $sort_b = intval($meta_values[$b->term_id]['sort'] ?? 0);
            return $sort_a - $sort_b;
        });
    }

    return $brands_by_letter;
}