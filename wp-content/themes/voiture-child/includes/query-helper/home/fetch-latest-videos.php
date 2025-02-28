<?php
function fetch_latest_videos_from_db($args)
{
	$post_type = $args['post_type'] ?? '';
    global $wpdb;

    // get only ids of the latest 5 videos
    $args = array(
        'post_type' => $post_type,
        'posts_per_page' => 5,
        'fields' => 'ids'
    );
    $video_query = new WP_Query($args);
    $post_ids = $video_query->posts;

    // get post meta for the given post IDs and meta_key = 'video_youtube_id'
    $meta_data = get_selected_meta_data_for_posts($post_ids, ['video_youtube_id']);

    $video_ids = [];
    foreach ($meta_data as $meta) {
        $video_ids[] = $meta['video_youtube_id'][0];
    }

    return $video_ids;
}
