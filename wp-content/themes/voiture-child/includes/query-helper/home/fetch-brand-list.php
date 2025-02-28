<?php

function fetch_brand_logo_data_from_db($taxonomy_type)
{
    $brands = [];

    if ($taxonomy_type === 'listing_make_evc') {
        $ev_brands = get_terms([
            'taxonomy'   => $taxonomy_type,
            'hide_empty' => false,
            'fields'     => 'id=>slug',
            'meta_key'   => 'state',
            'orderby'    => 'meta_value',
            'order'      => 'ASC',
        ]);

        $brand_names = array_values($ev_brands);
        $brands = get_terms([
            'taxonomy'   => 'listing_make',
            'slug'       => $brand_names,
            'hide_empty' => false,
            'fields'     => 'id=>name'
        ]);
    } else {
        $brands = get_terms([
            'taxonomy'   => $taxonomy_type,
            'hide_empty' => false,
            'fields'     => 'id=>name'
        ]);
    }

    if (is_wp_error($brands) || empty($brands)) {
        return [];
    }

    $term_ids = array_keys($brands);
    // Define which fields to fetch based on taxonomy type
    $fields = ($taxonomy_type === 'motorcycle_make')
        ? ['state', 'sort', 'motorcycle_make_image']
        : ['state', 'sort', 'listing_make_image'];

    $terms_data = fetch_terms_data($term_ids, $fields);

    $filtered_terms_data = array_filter($terms_data, fn($term) => $term['state'] == 1);

    if ($taxonomy_type === 'listing_make_evc') {
        $filtered_terms = [];
        foreach ($ev_brands as $brand_id => $brand_name) {
            foreach ($filtered_terms_data as $term) {
                if ($term['slug'] === $brand_name) {
                    $filtered_terms[] = $term;
                    break;
                }
            }
        }
        $final_sorted_brands = array_slice($filtered_terms, 0, 8);
    } else {
        usort($filtered_terms_data, fn($a, $b) => $a['sort'] <=> $b['sort']);
        $final_sorted_brands = array_slice($filtered_terms_data, 0, 8);
    }

    return array_map(fn($term_data) => [
        'brand'      => $brands[$term_data['term_id']],
        'slug'       => $term_data['slug'],
        'image_url'  => $term_data['listing_make_image'] ?? $term_data['motorcycle_make_image'] ?? '',
        'sort_order' => intval($term_data['sort']),
    ], $final_sorted_brands);
}
