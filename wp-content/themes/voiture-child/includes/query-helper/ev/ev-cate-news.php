<?php

function fetch_ev_car_review_news_from_db($args)
{
    global $wpdb;

    $posts_per_page = 4;
    $current_page = $args['paged'] ?? 1;
    $term_name = 'รีวิว';
    $taxonomy = 'news-category';

    // get Evs news category term
    $ev_parent_term = get_term_by_name_taxonomy_parent('รถยนต์ไฟฟ้า', $taxonomy, 0);
    if (!$ev_parent_term) {
        return [];
    }

    $sql = $wpdb->prepare(
        "SELECT t.* FROM {$wpdb->terms} AS t
        INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
        WHERE tt.taxonomy = %s
        AND t.name = %s
        AND tt.parent = %d",
        $taxonomy,
        $term_name,
        $ev_parent_term->term_id
    );

    $terms = $wpdb->get_results($sql);

    if (!empty($terms)) {
        $news_category_term = $terms[0];
    } else {
        return [];
    }

    $news_category_term_id = $news_category_term->term_id;
    $serialized_value = ':"' . $news_category_term_id . '";';

    $query_args = [
        'post_type'  => 'news',
        'meta_query' => [
            [
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '=',
            ],
            [
                'key'     => 'news_category',
                'value'   => $serialized_value,
                'compare' => 'LIKE',
            ],
            // publish_time has to be less than current time
            [
                'key'     => 'publish_time',
                'value'   => date('Y-m-d H:i:s'),
                'compare' => '<',
                'type'    => 'DATETIME',
            ],
            // weight time greater than current time
            // [
            //     'key'     => 'weight',
            //     'value'   => date('Y-m-d H:i:s'),
            //     'compare' => '<',
            //     'type'    => 'DATETIME',
            // ],
        ],
        'posts_per_page' => $posts_per_page,
        'paged'          => $current_page,
        // order by weight meta key descending
        // 'meta_key'       => 'weight',
        // order by publish time descending
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'meta_type'      => 'DATETIME',
        'order'          => 'DESC',
    ];

    $news_posts = new WP_Query($query_args);
    wp_reset_postdata();

    $news_data = formatted_news_data($news_posts->posts);

    return $news_data;
}

function fetch_ev_car_comparison_news_from_db($args)
{
    $posts_per_page = 4;
    $current_page = $args['paged'] ?? 1;
    $term_name = 'เปรียบเทียบ';
    $taxonomy = 'news-category';
    $ev_parent_term = get_term_by_name_taxonomy_parent('รถยนต์ไฟฟ้า', $taxonomy, 0);

    if (!$ev_parent_term) {
        return [];
    }

    $news_category_terms = get_all_terms_by_name_taxonomy($term_name, $taxonomy);
    $ev_news_category_terms = get_all_terms_by_parent($ev_parent_term->term_id);

    if (!$ev_news_category_terms || !$news_category_terms) {
        return [];
    }

    $meta_args = [
        'relation' => 'AND',
        [
            'key'     => 'second_language',
            'value'   => '',
            'compare' => '=',
        ],
    ];

    $ev_category_meta_query = [
        'relation' => 'OR',
    ];

    foreach ($ev_news_category_terms as $ev_news_category_term) {
        $ev_category_meta_query[] = [
            'key' => 'news_category',
            'value' => '"' . $ev_news_category_term->term_id . '"',
            'compare' => 'LIKE'
        ];
    }

    $meta_args[] = $ev_category_meta_query;

    $comparison_category_meta_query = [
        'relation' => 'OR',
    ];

    foreach ($news_category_terms as $news_category_term) {
        $comparison_category_meta_query[] = [
            'key' => 'news_category',
            'value' => '"' . $news_category_term->term_id . '"',
            'compare' => 'LIKE'
        ];
    }

    $meta_args[] = $comparison_category_meta_query;

    $query_args = [
        'post_type'      => 'news',
        'meta_query'     => $meta_args,
        'posts_per_page' => $posts_per_page,
        'paged'          => $current_page,
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'meta_type'      => 'DATETIME',
        'order'          => 'DESC',
    ];

    $news_posts = new WP_Query($query_args);

    $news_data = formatted_news_data($news_posts->posts);

    return $news_data;
}

function get_term_by_name_taxonomy($term_name, $taxonomy, $description = '')
{
    $response = get_all_terms_by_name_taxonomy($term_name, $taxonomy, $description);

    if ($response) {
        return $response[0];
    }

    return false;
}

function get_all_terms_by_name_taxonomy($term_name, $taxonomy, $description = '')
{
    $terms = get_terms(array(
        'taxonomy'   => $taxonomy,
        'name'       => $term_name,
        'hide_empty' => false,
        // 'description__like' => $description
    ));

    if (! is_wp_error($terms) && ! empty($terms)) {
        return $terms;
    }

    return false;
}
