<?php

function fetch_car_faq_data_from_db()
{
    $make = get_query_var('make');
    $get_make_data = false;

    if ($make) {
        $make_term = get_term_by('slug', $make, 'listing_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $args = array(
        'post_type'  => 'faq',
        'meta_query' => array(),
        'meta_key'   => 'weight',
        'orderby'    => 'meta_value',
        'order'      => 'ASC',
    );

    if ($get_make_data) {
        $brand_id = $make_term->term_id;
        $args['meta_query'] = array(
            'key'     => 'related_make',
            'value'   => $brand_id,
            'compare' => '='
        );
    }

    $faq_posts = get_posts($args);

    if (empty($faq_posts)) {
        return ['posts' => null, 'brand_name' => ''];
    }

    return ['posts' => $faq_posts, 'brand_name' => $make_term ? $make_term->name : ''];
}

function fetch_motor_faq_data_from_db()
{
    $make = get_query_var('make');
    $get_make_data = false;

    if ($make) {
        $make_term = get_term_by('slug', $make, 'motorcycle_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $args = array(
        'post_type'  => 'motorcycle-faq',
        'meta_query' => array(),
        'meta_key'   => 'weight',
        'orderby'    => 'meta_value',
        'order'      => 'ASC',
    );

    if ($get_make_data) {
        $brand_id = $make_term->term_id;
        $args['meta_query'] = array(
            'key'     => 'related_make',
            'value'   => $brand_id,
            'compare' => '='
        );
    }

    $faq_posts = get_posts($args);

    if (empty($faq_posts)) {
        return ['posts' => null, 'brand_name' => ''];
    }

    return ['posts' => $faq_posts, 'brand_name' => $make_term ? $make_term->name : ''];
}
