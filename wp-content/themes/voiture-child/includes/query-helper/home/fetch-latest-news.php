<?php

function fetch_single_thumbnail_news_data_from_db($args)
{
	$post_type = $args['post_type'] ?? '';
	
    $args = array(
        'post_type' => $post_type,
        'posts_per_page' => 1,
        'post_status' => 'publish',
        'meta_query' => array(
            'relation' => 'AND',
            array(
                'key' => 'second_language',
                'value' => '',
                'compare' => '==',
            ),
            array(
                'key'     => 'publish_time',
                'value'   => current_time('mysql'),
                'compare' => '<',
                'type'    => 'DATETIME'
            )
        ),
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'order'          => 'DESC',
        'meta_type'      => 'DATETIME',
    );

    $news_query = new WP_Query($args);
    if (!$news_query->have_posts()) {
        return [];
    }

    $news_posts = $news_query->posts;
    $post = $news_posts[0];

    $required_meta_keys = ['_thumbnail_id', 'news_category'];
    $required_meta = get_selected_meta_data_for_posts([$post->ID], $required_meta_keys);

    $thumbnail_id = $required_meta[$post->ID]['_thumbnail_id'][0] ?? null;
    $image_post = $thumbnail_id ? get_post($thumbnail_id) : null;
    $thumbnail_url = $image_post ? $image_post->guid : null;

    //category
    $serialized_category = $required_meta[$post->ID]['news_category'][0] ?? null;
    $category_data = $serialized_category ? unserialize($serialized_category) : null;
    $category_id = $category_data[0] ?? null;
    $news_category = $category_id ? get_term($category_id)->name : 'ข่าว';

    $news_post = [
        'id' => $post->ID,
        'title' => $post->post_title,
        'link'  => get_custom_post_link($post->ID, ''),
        'thumbnail_url' => $thumbnail_url,
        'news_category' => $news_category ? $news_category : 'ข่าว'
    ];

    return $news_post;
}

function fetch_latest_news_data_from_db($args)
{
	$post_type = $args['post_type'] ?? '';
    $args = array(
        'post_type'      => $post_type,
        'posts_per_page' => 5,
        'post_status'    => 'publish',
        'meta_query' => array(
            'relation' => 'AND',
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '=='
            ),
            array(
                'key'     => 'publish_time',
                'value'   => current_time('mysql'),
                'compare' => '<',
                'type'    => 'DATETIME'
            )
        ),
        'offset'         => 1,
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'order'          => 'DESC',
        'meta_type'      => 'DATETIME',
    );

    $latest_news_data = new WP_Query($args);

    if ($latest_news_data->have_posts()) {
        $latest_news = [];
        $post_ids = wp_list_pluck($latest_news_data->posts, 'ID');
        $all_meta_data = get_post_meta_with_thumbnail_guid($post_ids);

        foreach ($latest_news_data->posts as $post) {
            $news_id = $post->ID;
            $title = $post->post_title;
            $content = wp_trim_words($post->post_content, 20, '...');

            // post meta
            $post_meta = $all_meta_data[$news_id] ?? [];
            $publish_time = $post_meta['publish_time'][0] ?? '';
            $thumbnail_url = $post_meta['_thumbnail_guid'] ?? CAR_PLACEHOLDER;

            // Author details
            $author_id = $post->post_author;
            $author = get_the_author_meta('display_name', $author_id);

            $author_image_url = get_image_url($author_id, 'author');
            $author_page_link = get_author_posts_url($author_id);
            $custom_author_link = get_custom_author_post_link($author_id, $author_page_link);

            $latest_news[] = [
                'id' => $news_id,
                'title' => $title,
                'author' => $author,
                'content' => $content,
                'thumbnail_url' => $thumbnail_url,
                'post_date' => $publish_time,
                'avatar' => $author_image_url,
                'link'  => get_custom_post_link($news_id, ''),
                'custom_author_link' => $custom_author_link,
            ];
        }
    }

    return $latest_news;
}

function fetch_category_news_from_db($args)
{
    global $wpdb;
    $current_page = $args['paged'] ?? 1;
    $posts_per_page = 5;
    $news_category = $args['category'];
    $taxonomy = 'news-category';
    $description = $args['description'] ?? '';

    $sql = $wpdb->prepare(
        "
        SELECT t.*, tt.*
        FROM {$wpdb->terms} AS t
        INNER JOIN {$wpdb->term_taxonomy} AS tt
        ON t.term_id = tt.term_id
        WHERE tt.taxonomy = %s
        AND t.name = %s
        AND tt.description = %s ",
        $taxonomy,
        $news_category,
        $description
    );
    $terms = $wpdb->get_results($sql);

    if (!empty($terms)) {
        $news_category_term = $terms[0];
    } else {
        return [];
    }

    $news_category_term_id = $news_category_term->term_id;
    $serialized_value = ':"' . $news_category_term_id . '";';
//     $second_language = get_current_language();

    $query_args = [
        'post_type'  => 'news',
        'post_status' => 'publish',
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
    if (!$news_posts->have_posts()) {
        return [];
    }

    $news = [];
    $post_ids = wp_list_pluck($news_posts->posts, 'ID');
    $required_meta = get_selected_meta_data_for_posts($post_ids, ['publish_time', '_thumbnail_id']);
    $post_author_ids = wp_list_pluck($news_posts->posts, 'post_author');
    $author_data = get_author_data($post_author_ids);
    $thumbnail_urls = get_post_thumbnail_urls($post_ids);

    foreach ($news_posts->posts as $post) {
        $news_id = $post->ID;
        $title = $post->post_title;
        $content = wp_trim_words($post->post_content, 20, '...');
        $author_id = $post->post_author;

        // post meta
        $post_meta = $required_meta[$news_id] ?? [];
        $publish_time = $post_meta['publish_time'][0] ?? '';
        // $thumbnail_id = $post_meta['_thumbnail_id'][0] ?? '';
        $thumbnail_url = $thumbnail_urls[$news_id] ?? '';
        $author_name = $author_data[$author_id]['display_name'] ?? '';
        $author_image_url = $author_data[$author_id]['author_image_url'] ?? '';

        $news[] = [
            'id' => $news_id,
            'title' => $title,
            'author' => $author_name,
            'content' => $content,
            'thumbnail_url' => $thumbnail_url,
            'post_date' => convert_myt_to_ist($publish_time),
            'author_img' => $author_image_url,
            'link'  => get_custom_post_link($news_id, ''),
        ];
    }

    return $news;
}

function fetch_fuel_consumption_news_from_db()
{
    // get 5 news posts whose title has fuel consumption or fuel-consumption in it
    $args = array(
        'post_type'      => 'news',
        'post_status'    => 'publish',
        'posts_per_page' => 5,
        'orderby'        => 'date',
        'order'          => 'DESC',
        's'             => 'fuel consumption fuel-consumption',
        'meta_query'     => array(
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '=',
            ),
            array(
                'key'     => 'publish_time',
                'value'   => current_time('mysql'),
                'compare' => '<',
                'type'    => 'DATETIME',
            ),
        ),
    );

    $fuel_news_data = new WP_Query($args);
    if (!$fuel_news_data->have_posts()) {
        return [];
    }

    $fuel_news = [];
    $post_ids = wp_list_pluck($fuel_news_data->posts, 'ID');
    $all_meta_data = get_post_meta_with_thumbnail_guid($post_ids);
    foreach ($fuel_news_data->posts as $post) {
        $news_id = $post->ID;
        $title = $post->post_title;
        $content = wp_trim_words($post->post_content, 20, '...');

        // post meta
        $post_meta = $all_meta_data[$news_id] ?? [];
        $publish_time = $post_meta['publish_time'][0] ?? '';
        $thumbnail_url = $post_meta['_thumbnail_guid'] ?? '';

        $fuel_news[] = [
            'id' => $news_id,
            'title' => $title,
            'content' => $content,
            'thumbnail_url' => $thumbnail_url,
            'post_date' => convert_myt_to_ist($publish_time),
            'link'  => get_custom_post_link($news_id, ''),
        ];
    }

    return $fuel_news;
}

function fetch_related_tag_news_data_from_db($args)
{
    global $wpdb;

    $tag = $args['tag'];
    $paged = $args['paged'] ?? 1;

    // taxonomy is all-tags
    $query = "SELECT * FROM wp_terms WHERE slug = '" . esc_sql($tag) . "' AND term_id IN (SELECT term_id FROM wp_term_taxonomy WHERE taxonomy = 'all-tags')";
    $term = $wpdb->get_row($query);
    if (!$term || is_wp_error($term)) {
        return [];
    }
    $related_news_term_id = $term->term_id;

    // Set up the query to get the first 5 posts
    $args = array(
        'post_type' => 'news',
        'post_status' => 'publish',
        'posts_per_page' => 5,
        'paged' => $paged,
        'meta_query' => array(
            'relation' => 'AND', // Combine both conditions
            array(
                'key' => 'tags',
                'value' => sprintf(':"%d";', $related_news_term_id),
                'compare' => 'LIKE',
            ),
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            )
        ),
    );

    $news_posts = new WP_Query($args);

    if (!$news_posts->have_posts()) {
        return [];
    }

    $news = [];
    $post_ids = wp_list_pluck($news_posts->posts, 'ID');
    $required_meta = get_selected_meta_data_for_posts($post_ids, ['publish_time', '_thumbnail_id']);
    $post_author_ids = wp_list_pluck($news_posts->posts, 'post_author');
    $author_data = get_author_data($post_author_ids);
    $thumbnail_urls = get_post_thumbnail_urls($post_ids);

    foreach ($news_posts->posts as $post) {
        $news_id = $post->ID;
        $title = $post->post_title;
        $content = wp_trim_words($post->post_content, 20, '...');
        $author_id = $post->post_author;

        // post meta
        $post_meta = $required_meta[$news_id] ?? [];
        $publish_time = $post_meta['publish_time'][0] ?? '';
        $thumbnail_url = $thumbnail_urls[$news_id] ?? '';
        $author_name = $author_data[$author_id]['display_name'] ?? '';
        $author_image_url = $author_data[$author_id]['author_image_url'] ?? '';

        $news[] = [
            'id' => $news_id,
            'title' => $title,
            'author' => $author_name,
            'content' => $content,
            'thumbnail_url' => $thumbnail_url,
            'post_date' => convert_myt_to_ist($publish_time),
            'author_img' => $author_image_url,
            'link'  => get_custom_post_link($news_id, ''),
        ];
    }

    return $news;
}

function fetch_motor_category_news_from_db($args)
{
    global $wpdb;
    $current_page = $args['paged'] ?? 1;
    $posts_per_page = 5;
    $news_category = $args['category'];
    $taxonomy = 'motorcycle-news-category';
    $description = $args['description'] ?? '';
	
	  $sql = $wpdb->prepare(
		"SELECT t.*, tt.*
		FROM {$wpdb->terms} AS t
		INNER JOIN {$wpdb->term_taxonomy} AS tt
		ON t.term_id = tt.term_id
		WHERE tt.taxonomy = %s
		AND (
			(t.name = %s AND tt.description = %s)
			OR
			(t.name = 'Others' AND tt.parent = (
				SELECT parent_tt.term_id
				FROM {$wpdb->terms} AS parent_t
				INNER JOIN {$wpdb->term_taxonomy} AS parent_tt
				ON parent_t.term_id = parent_tt.term_id
				WHERE parent_tt.taxonomy = %s
				AND parent_t.name = %s
				LIMIT 1
			))
		)",
		$taxonomy,
		$news_category,
		$description,
		$taxonomy,
		$news_category
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
        'post_type'  => 'motorcycle-news',
        'meta_query' => [
            [
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '=',
            ],
            [
                'key'     => 'motorcycle-news-category',
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
    if (!$news_posts->have_posts()) {
        return [];
    }

    $news = [];
    $post_ids = wp_list_pluck($news_posts->posts, 'ID');
    $required_meta = get_selected_meta_data_for_posts($post_ids, ['publish_time', '_thumbnail_id']);
    $post_author_ids = wp_list_pluck($news_posts->posts, 'post_author');
    $author_data = get_author_data($post_author_ids);
    $thumbnail_urls = get_post_thumbnail_urls($post_ids);

    foreach ($news_posts->posts as $post) {
        $news_id = $post->ID;
        $title = $post->post_title;
        $content = wp_trim_words($post->post_content, 20, '...');
        $author_id = $post->post_author;

        // post meta
        $post_meta = $required_meta[$news_id] ?? [];
        $publish_time = $post_meta['publish_time'][0] ?? '';
        // $thumbnail_id = $post_meta['_thumbnail_id'][0] ?? '';
        $thumbnail_url = $thumbnail_urls[$news_id] ?? '';
        $author_name = $author_data[$author_id]['display_name'] ?? '';
        $author_image_url = $author_data[$author_id]['author_image_url'] ?? '';

        $news[] = [
            'id' => $news_id,
            'title' => $title,
            'author' => $author_name,
            'content' => $content,
            'thumbnail_url' => $thumbnail_url,
            'post_date' => convert_myt_to_ist($publish_time),
            'author_img' => $author_image_url,
            'link'  => get_custom_post_link($news_id, ''),
        ];
    }

    return $news;
}