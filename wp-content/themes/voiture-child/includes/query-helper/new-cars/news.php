<?php

function  fetch_car_news_data_from_db()
{
    global $wpdb;

    $make = get_query_var('make');
    $get_make_data = false;

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'listing_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $args = [
        'post_type'      => 'news',
        'posts_per_page' => 6,
        'meta_query'     => [
            'relation' => 'AND',
            [
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            ],
            [
                'key' => 'publish_time',
                'value'   => current_time('mysql'),
                'compare' => '<=',
                'type' => 'DATETIME'
            ]
        ],
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'order'          => 'DESC',
        'meta_type'      => 'DATETIME',
    ];

    if ($get_make_data) {
        $brand_id = $make_term->term_id;
        $listing_ids = $brand_id > 0 ? $wpdb->get_col($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} p
         INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
         WHERE p.post_type = 'listing' 
         AND pm.meta_key = '_listing_make' 
         AND pm.meta_value = %d",
            $brand_id
        )) : [];

        if (!empty($listing_ids)) {
            $listing_conditions = [
                'relation' => 'OR'
            ];

            foreach ($listing_ids as $listing_id) {
                $listing_conditions[] = [
                    'key'     => 'related_car_model',
                    'value'   => '"' . $listing_id . '"',
                    'compare' => 'LIKE'
                ];
            }

            // Add the OR conditions array to the main meta_query
            $args['meta_query'][] = $listing_conditions;
        }
    }

    $news_posts = new WP_Query($args);
    $news_posts = $news_posts->have_posts() ? $news_posts : null;

    $post_ids = wp_list_pluck($news_posts->posts, 'ID');
    //     $post_ids = implode(',', $post_ids);
    $thumbnail_urls = get_post_thumbnail_urls($post_ids);

    // permalink, thumbnail_url
    foreach ($news_posts->posts as $post) {
        $post->thumbnail_url = isset($thumbnail_urls[$post->ID]) ? $thumbnail_urls[$post->ID] : '';
        $post->permalink = get_custom_post_link($post->ID);
    }


    return ['posts' => $news_posts, 'brand_name' =>  $make ? $make_term->name : ''];
}

function  fetch_motor_news_data_from_db()
{
    global $wpdb;

    $make = get_query_var('make');
    $get_make_data = false;

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'motorcycle_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $args = [
        'post_type'      => 'motorcycle-news',
        'posts_per_page' => 6,
        'meta_query'     => [
            'relation' => 'AND',
            [
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            ],
            [
                'key' => 'publish_time',
                'value'   => current_time('mysql'),
                'compare' => '<=',
                'type' => 'DATETIME'
            ]
        ],
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'order'          => 'DESC',
        'meta_type'      => 'DATETIME',
    ];

    if ($get_make_data) {
        $brand_id = $make_term->term_id;
        $listing_ids = $brand_id > 0 ? $wpdb->get_col($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} p
         INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
         WHERE p.post_type = 'motorcycle-listing' 
         AND pm.meta_key = 'make' 
         AND pm.meta_value = %d",
            $brand_id
        )) : [];

        if (!empty($listing_ids)) {
            $listing_conditions = [
                'relation' => 'OR'
            ];

            foreach ($listing_ids as $listing_id) {
                $listing_conditions[] = [
                    'key'     => 'related_bike_model',
                    'value'   => '"' . $listing_id . '"',
                    'compare' => 'LIKE'
                ];
            }

            // Add the OR conditions array to the main meta_query
            $args['meta_query'][] = $listing_conditions;
        }
    }

    $news_posts = new WP_Query($args);
    $news_posts = $news_posts->have_posts() ? $news_posts : null;

    $post_ids = wp_list_pluck($news_posts->posts, 'ID');
    //     $post_ids = implode(',', $post_ids);
    $thumbnail_urls = get_post_thumbnail_urls($post_ids);

    // permalink, thumbnail_url
    foreach ($news_posts->posts as $post) {
        $post->thumbnail_url = isset($thumbnail_urls[$post->ID]) ? $thumbnail_urls[$post->ID] : '';
        $post->permalink = get_custom_post_link($post->ID);
    }


    return ['posts' => $news_posts, 'brand_name' =>  $make ? $make_term->name : ''];
}
