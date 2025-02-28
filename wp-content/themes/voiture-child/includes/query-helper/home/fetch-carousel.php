<?php

function fetch_carousel_data_from_db($args = array()) {
    $default_args = array(
        'post_type'      => 'banner',
        'posts_per_page' => 10,
        'orderby'        => 'date',
        'order'          => 'DESC',
        'post_status'    => 'publish',
        'fields'         => 'ids', // Only fetch post IDs to reduce query load
    );

    $args = wp_parse_args($args, $default_args);
    $current_datetime = current_time('Y-m-d H:i:s');

    // Add meta query for start and end times, as well as status
    $args['meta_query'] = array(
        'relation' => 'AND',
        array(
            'key'     => 'effective_start_time',
            'value'   => $current_datetime,
            'compare' => '<=',
            'type'    => 'DATETIME', // Use DATETIME if stored correctly in the database
        ),
        array(
            'key'     => 'effective_end_time',
            'value'   => $current_datetime,
            'compare' => '>=',
            'type'    => 'DATETIME', // Use DATETIME if stored correctly in the database
        ),
        array(
            'key'     => 'status',
            'value'   => 'Enable',
            'compare' => '=',
        ),
    );

    // Fetch posts based on query args
    $carousel_query = new WP_Query($args);
    $carousel_data  = array();

    if ($carousel_query->have_posts()) {
        $post_ids = $carousel_query->posts; // Get post IDs from query

        // Fetch all custom fields in one go using get_fields
        foreach ($post_ids as $post_id) {
            // Fetch all custom fields at once
            $fields = get_fields($post_id);

            if ($fields) {
                $banner_image         = isset($fields['picture']) ? $fields['picture'] : null;
                $banner_title         = isset($fields['title']) ? $fields['title'] : '';
                $banner_url           = isset($fields['url']) ? $fields['url'] : '';
                $effective_start_time = isset($fields['effective_start_time']) ? $fields['effective_start_time'] : '';
                $effective_end_time   = isset($fields['effective_end_time']) ? $fields['effective_end_time'] : '';

                // Get image GUID if the image is available
                $image_guid = '';
                if (!empty($banner_image['ID'])) {
                    $image_post = get_post($banner_image['ID']);
                    $image_guid = !empty($image_post) ? $image_post->guid : '';
                }

                if (!empty($image_guid)) {
                    // Add data to the carousel array
                    $carousel_data[] = array(
                        'image_guid'         => $image_guid,
                        'title'              => $banner_title,
                        'url'                => $banner_url,
                        'effective_end_time' => $effective_end_time,
                        'effective_start_time'=> $effective_start_time,
                    );
                }
            }
        }
        wp_reset_postdata();
    }

    return $carousel_data;
}
