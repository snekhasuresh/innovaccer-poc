<?php

function fetch_latest_ev_news_data_from_db() 
{
    // get Evs news category term
    $taxonomy = 'news-category';
    $ev_parent_term = get_term_by_name_taxonomy_parent('รถยนต์ไฟฟ้า', $taxonomy, 0);

    // get all sub categories of EVs
    $evs_news_category_terms = get_all_terms_by_parent($ev_parent_term->term_id);
    $latest_news_meta_args = [
        'relation' => 'AND',
        [
            'key' => 'second_language',
            'value' => '',
            'compare' => '='
        ],
        // meta key publish_time less than current time
        [
            'key' => 'publish_time',
            'value' => date('Y-m-d H:i:s'),
            'compare' => '<'
        ]
    ];

    $category_meta_query = [
        'relation' => 'OR',
    ];

    foreach ($evs_news_category_terms as $evs_news_category_term) {
        $category_meta_query[] = [
            'key' => 'news_category',
            'value' => '"' . $evs_news_category_term->term_id . '"',
            'compare' => 'LIKE'
        ];
    }

    $latest_news_meta_args[] = $category_meta_query;

    $latest_news_args = array(
        'post_type' => 'news',
        'meta_query' => $latest_news_meta_args,
        'posts_per_page' => 5,
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'meta_type'      => 'DATETIME',
        'order'          => 'DESC',
    );

    $latest_news = new WP_Query($latest_news_args);
    $latest_news_data = [];
    if ($latest_news->have_posts()) {
        $post_ids = wp_list_pluck($latest_news->posts, 'ID');
        $all_meta_data = get_post_meta_with_thumbnail_guid($post_ids);
        foreach ($latest_news->posts as $post) {
            $title = $post->post_title;
            $content = wp_trim_words($post->post_content, 20, '...');
            $thumbnail_url = $all_meta_data[$post->ID]['_thumbnail_guid'] ?? CAR_PLACEHOLDER;

            $author_id = $post->post_author;
            $author_name = get_the_author_meta('display_name', $author_id);
            $author_image_url = get_the_author_meta('user_url', $author_id);
            if (!$author_image_url) {
                $author_image_url = get_avatar_url($author_id, ['size' => 32]);
            }

            $author_page_link = get_author_posts_url($author_id);
            $custom_author_link = get_custom_author_post_link($author_id, $author_page_link);

            $latest_news_data[] = [
                'id' => $post->ID,
                'title' => $title,
                'author' => $author_name,
                'content' => $content,
                'thumbnail_url' => $thumbnail_url,
                'publish_time' => convert_myt_to_ist($all_meta_data[$post->ID]['publish_time'][0] ?? ''),
                'avatar' => $author_image_url,
                'custom_author_link' => $custom_author_link,
                'link'  => get_custom_post_link($post->ID, ''),
            ];
        }
    }

    return $latest_news_data;
}

