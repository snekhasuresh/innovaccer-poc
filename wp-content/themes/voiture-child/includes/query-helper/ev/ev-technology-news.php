<?php

function fetch_ev_technology_news_from_db()
{
    global $wpdb;
    $current_page = 1;
    $posts_per_page = 5;
    $news_category = 'เทคโนโลยี';
    $taxonomy = 'news-category';

    // get Evs news category term
    $taxonomy = 'news-category';
    $ev_parent_term = get_term_by_name_taxonomy_parent('รถยนต์ไฟฟ้า', $taxonomy, 0);
    if (!$ev_parent_term) {
        return [];
    }

    $term_name = $news_category;
    $parent = $ev_parent_term->term_id;
    $sql = $wpdb->prepare(
        "SELECT t.* FROM {$wpdb->terms} AS t
            INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
            WHERE tt.taxonomy = %s
            AND t.name = %s
            AND tt.parent = %d",
        $taxonomy,
        $term_name,
        $parent
    );

    $terms = $wpdb->get_results($sql);

    if (!empty($terms)) {
        $news_category_term = $terms[0];
    } else {
        return [];
    }

    if ($news_category_term) {
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
    } else {
        return [];
    }

    $news_data = formatted_news_data($news_posts->posts);

    return $news_data;
}
