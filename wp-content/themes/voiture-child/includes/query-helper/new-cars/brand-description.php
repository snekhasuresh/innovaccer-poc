<?php

function fetch_brand_description_data_from_db()
{
    global $wpdb;

    $brand_slug = get_query_var('make');
    if (!$brand_slug) {
        return 'Brand not found.';
    }
    if ($brand_slug) {
        $brand = get_term_by('slug', $brand_slug, 'listing_make');

        if (!$brand) {
            return 'Brand not found.';
        }
        $brand_id = $brand->term_id;
    }

    $brand_image = get_term_meta($brand_id, 'listing_make_image', true);
    $brand_info = [];
    $brand_info['brand_name'] = $brand->name;
    $brand_info['brand_description'] = $brand->description;
    $brand_info['brand_image'] = $brand_image ? $brand_image : '';

    if (!$brand_info) {
        return null;
    }

    // Process description
    $description = strip_tags($brand_info['brand_description']);
    $preview_text = $remaining_text = '';
    if (!empty($description)) {
        $initial_text = mb_substr($description, 0, 1000);
        $last_full_stop = mb_strrpos($initial_text, '.');

        if ($last_full_stop !== false) {
            $preview_text = mb_substr($description, 0, $last_full_stop + 1);
            $remaining_text = mb_substr($description, $last_full_stop + 1);
        } else {
            $preview_text = $initial_text;
            $remaining_text = mb_substr($description, 1000);
        }
    }

    // Fetch models and prices in a single optimized query
    $models_query = $wpdb->prepare(
        "SELECT 
            tt.term_id AS type_id,
            t.name AS type_name,
            p.ID AS model_id,
            p.post_title AS model_name,
            pm_state.meta_value AS listing_state,
            MAX(CAST(pm_price.meta_value AS UNSIGNED)) AS max_retail_price
        FROM {$wpdb->posts} p
        INNER JOIN {$wpdb->postmeta} pm_make ON p.ID = pm_make.post_id AND pm_make.meta_key = '_listing_make' AND pm_make.meta_value = %d
        INNER JOIN {$wpdb->term_relationships} tr ON p.ID = tr.object_id
        INNER JOIN {$wpdb->term_taxonomy} tt ON tr.term_taxonomy_id = tt.term_id AND tt.taxonomy = 'listing_type'
        INNER JOIN {$wpdb->terms} t ON tt.term_id = t.term_id
        LEFT JOIN {$wpdb->postmeta} pm_state ON p.ID = pm_state.post_id AND pm_state.meta_key = 'listing-state'
        LEFT JOIN {$wpdb->posts} variants ON variants.post_parent = p.ID AND variants.post_type = 'variant'
        LEFT JOIN {$wpdb->postmeta} pm_price ON variants.ID = pm_price.post_id AND pm_price.meta_key = 'retail_price'
        WHERE p.post_type = 'listing'
        GROUP BY p.ID, tt.term_id, t.name
        ORDER BY t.name, p.post_title",
        $brand_id
    );
    $models_data = $wpdb->get_results($models_query, ARRAY_A);

    // Process models
    $grouped_models = [];
    $models_with_prices = [];
    foreach ($models_data as $model) {
        $grouped_models[$model['type_name']][] = $model['model_name'];

        $price_info = get_price_range_of_listing($model['model_id']);

        $models_with_prices[] = [
            'name' => $model['model_name'],
            'price' => $price_info
        ];
    }

    // Remove duplicates
    foreach ($grouped_models as &$models) {
        $models = array_unique($models);
    }
    $models_with_prices = array_values(array_intersect_key(
        $models_with_prices,
        array_unique(array_column($models_with_prices, 'name'))
    ));

    $brand_data = [
        'brand_name' => $brand_info['brand_name'],
        'brand_image' => $brand_info['brand_image'],
        'preview_text' => $preview_text,
        'remaining_text' => $remaining_text,
        'grouped_models' => $grouped_models,
        'models_with_prices' => $models_with_prices
    ];

    return $brand_data;
}

function fetch_motor_brand_description_data_from_db()
{
    global $wpdb;

    $brand_slug = get_query_var('make');
    if (!$brand_slug) {
        return 'Brand not found.';
    }
    if ($brand_slug) {
        $brand = get_term_by('slug', $brand_slug, 'motorcycle_make');

        if (!$brand) {
            return 'Brand not found.';
        }
        $brand_id = $brand->term_id;
    }

    $brand_image = get_term_meta($brand_id, 'motorcycle_make_image', true);
    $brand_info = [];
    $brand_info['brand_name'] = $brand->name;
    $brand_info['brand_description'] = $brand->description;
    $brand_info['brand_image'] = $brand_image ? $brand_image : '';

    if (!$brand_info) {
        return null;
    }

    // Process description
    $description = strip_tags($brand_info['brand_description']);
    $preview_text = $remaining_text = '';
    if (!empty($description)) {
        $initial_text = mb_substr($description, 0, 500);
        $last_full_stop = mb_strrpos($initial_text, '.');

        if ($last_full_stop !== false) {
            $preview_text = mb_substr($description, 0, $last_full_stop + 1);
            $remaining_text = mb_substr($description, $last_full_stop + 1);
        } else {
            $preview_text = $initial_text;
            $remaining_text = mb_substr($description, 500);
        }
    }

    // Fetch models and prices in a single optimized query
    $models_query = $wpdb->prepare(
        "SELECT 
            tt.term_id AS type_id,
            t.name AS type_name,
            p.ID AS model_id,
            p.post_title AS model_name,
            pm_state.meta_value AS listing_state,
            MAX(CAST(pm_price.meta_value AS UNSIGNED)) AS max_retail_price
        FROM {$wpdb->posts} p
        INNER JOIN {$wpdb->postmeta} pm_make ON p.ID = pm_make.post_id AND pm_make.meta_key = 'make' AND pm_make.meta_value = %d
        INNER JOIN {$wpdb->term_relationships} tr ON p.ID = tr.object_id
        INNER JOIN {$wpdb->term_taxonomy} tt ON tr.term_taxonomy_id = tt.term_id AND tt.taxonomy = 'motorcycle-listing-type'
        INNER JOIN {$wpdb->terms} t ON tt.term_id = t.term_id
        LEFT JOIN {$wpdb->postmeta} pm_state ON p.ID = pm_state.post_id AND pm_state.meta_key = 'listing_state'
        LEFT JOIN {$wpdb->posts} variants ON variants.post_parent = p.ID AND variants.post_type = 'motorcycle-variant'
        LEFT JOIN {$wpdb->postmeta} pm_price ON variants.ID = pm_price.post_id AND pm_price.meta_key = 'price'
        WHERE p.post_type = 'motorcycle-listing'
        GROUP BY p.ID, tt.term_id, t.name
        ORDER BY t.name, p.post_title",
        $brand_id
    );
    $models_data = $wpdb->get_results($models_query, ARRAY_A);

    // Process models
    $grouped_models = [];
    $models_with_prices = [];
    foreach ($models_data as $model) {
        $grouped_models[$model['type_name']][] = $model['model_name'];

        $price_info = get_motor_price_range_of_listing($model['model_id']);

        $models_with_prices[] = [
            'name' => $model['model_name'],
            'price' => $price_info
        ];
    }

    // Remove duplicates
    foreach ($grouped_models as &$models) {
        $models = array_unique($models);
    }
    $models_with_prices = array_values(array_intersect_key(
        $models_with_prices,
        array_unique(array_column($models_with_prices, 'name'))
    ));

    $brand_data = [
        'brand_name' => $brand_info['brand_name'],
        'brand_image' => $brand_info['brand_image'],
        'preview_text' => $preview_text,
        'remaining_text' => $remaining_text,
        'grouped_models' => $grouped_models,
        'models_with_prices' => $models_with_prices
    ];

    return $brand_data;
}
