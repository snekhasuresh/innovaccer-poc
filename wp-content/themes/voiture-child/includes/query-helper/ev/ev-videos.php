<?php

function fetch_latest_ev_videos_data_from_db()
{
    global $wpdb;

    $ev_cars = get_posts(array(
        'post_type' => 'listing',
        'meta_query' => array(
            array(
                'key' => 'is_ev',
                'value' => '1',
                'compare' => '=',
            ),
        ),
        'numberposts' => -1,
    ));

    $ev_car_ids = array();
    foreach ($ev_cars as $ev_car) {
        $ev_car_ids[] = $ev_car->ID;
    }

    $video_args = array(
        'post_type'  => 'video',
        'numberposts' => -1,
    );
    $videos = get_posts($video_args);

    $video_ids = array();
    foreach ($videos as $video) {
        // $video_id = get_post_meta($video->ID, 'video_youtube_id', true);
        $related_cars = get_post_meta($video->ID, 'related_car_model', true);
        if (!empty($related_cars)) {
            foreach ($related_cars as $car_id) {
                if (in_array($car_id, $ev_car_ids)) {
                    $video_ids[] = $video->ID;
                }
            }
        }
    }

    $related_video_args = array(
        'post_type'  => 'video',
        'post__in'   => $video_ids,
        // order by weight
        'meta_key'   => 'weight',
        'order'      => 'DESC',
        'orderby'    => 'meta_value_num',
        'posts_per_page' => 6,
    );
    $video_query = new WP_Query($related_video_args);

    $video_posts = $video_query->posts;
    $post_ids = implode(',', $video_ids);
    // get post meta for the given post IDs and meta_key = 'video_youtube_id'
    $query = "
    SELECT post_id, meta_value
    FROM {$wpdb->postmeta}
    WHERE meta_key = 'video_youtube_id'
    AND post_id IN ($post_ids)
";

    $youtube_ids = $wpdb->get_results($query, ARRAY_A);

    $video_response = array();
    foreach ($video_posts as $video_post) {
        $video_youtube_id = '';
        foreach ($youtube_ids as $youtube_id) {
            if ($youtube_id['post_id'] == $video_post->ID) {
                $video_youtube_id = $youtube_id['meta_value'];
                break;
            }
        }
        $video_response[] = array(
            'id' => $video_post->ID,
            'title' => $video_post->post_title,
            'video_youtube_id' => $video_youtube_id,
        );
    }

    return $video_response;
}
