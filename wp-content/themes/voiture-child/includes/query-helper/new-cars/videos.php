<?php

function fetch_cars_videos_data_from_db()
{
    $make = get_query_var('make');
    $get_make_data = false;

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'listing_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $args = array(
        'post_type'      => 'video',
        'posts_per_page' => 10,
        'orderby'        => 'post_modified',
        'order'          => 'DESC',
    );

    if ($get_make_data) {
        $brand_id = $make_term->term_id;
        $listing_ids = wp_list_pluck(get_posts([
            'post_type'      => 'listing',
            'posts_per_page' => -1,
            'meta_query'     => [['key' => '_listing_make', 'value' => $brand_id, 'compare' => '=']]
        ]), 'ID');

        if (!empty($listing_ids)) {
            $meta_query = ['relation' => 'OR'];

            foreach ($listing_ids as $listing_id) {
                $meta_query[] = [
                    'key'     => 'related_car_model',
                    'value'   => '"' . $listing_id . '"',
                    'compare' => 'LIKE'
                ];
            }

            // Add meta_query to args only if listings exist
            $args['meta_query'] = $meta_query;
        }
    }
    $videos = get_posts($args);
    if (empty($videos)) {
        return ['posts' => null, 'brand_name' => ''];
    }

    $required_meta = get_selected_meta_data_for_posts(wp_list_pluck($videos, 'ID'), ['video_youtube_id']);
    // add video_youtube_id to each post
    foreach ($videos as $key => $video) {
        $videos[$key]->video_youtube_id = $required_meta[$video->ID]['video_youtube_id'][0];
    }

    return ['posts' => $videos, 'brand_name' => $make_term ? $make_term->name : ''];
}

function fetch_motor_videos_data_from_db()
{
    $make = get_query_var('make');
    $get_make_data = false;

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'motorcycle_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $args = array(
        'post_type'      => 'motocycle-video',
        'posts_per_page' => 10,
        'orderby'        => 'post_modified',
        'order'          => 'DESC',
    );

    if ($get_make_data) {
        $brand_id = $make_term->term_id;
        $listing_ids = wp_list_pluck(get_posts([
            'post_type'      => 'motorcycle-listing',
            'posts_per_page' => -1,
            'meta_query'     => [['key' => 'make', 'value' => $brand_id, 'compare' => '=']]
        ]), 'ID');

        if (!empty($listing_ids)) {
            $meta_query = ['relation' => 'OR'];

            foreach ($listing_ids as $listing_id) {
                $meta_query[] = [
                    'key'     => 'related_bike_model',
                    'value'   => '"' . $listing_id . '"',
                    'compare' => 'LIKE'
                ];
            }
            // Add meta_query to args only if listings exist
            $args['meta_query'] = $meta_query;
        }
    }

    $videos = get_posts($args);

    if (empty($videos)) {
        return ['posts' => null, 'brand_name' => ''];
    }

    $required_meta = get_selected_meta_data_for_posts(wp_list_pluck($videos, 'ID'), ['video_youtube_id']);
    // add video_youtube_id to each post
    foreach ($videos as $key => $video) {
        $videos[$key]->video_youtube_id = $required_meta[$video->ID]['video_youtube_id'][0];
    }

    return ['posts' => $videos, 'brand_name' => $make_term ? $make_term->name : ''];
}
