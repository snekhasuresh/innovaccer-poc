<?php


/**
 * Get cache keys grouped by post type.
 *
 * This function returns the cache keys for the given post type.
 *
 * @param string $post_type The post type for which to fetch cache keys.
 * @return array Cache keys associated with the provided post type.
 */
function get_cache_keys_grouped_by_post_type($post_type, $post_id = 0)
{
    $cache_keys = [
        'banner' => [
            'banner_carousel' => ['function' => 'fetch_carousel_data_from_db', 'args' => []],
        ],
        'upcoming-car' => [
            'latest_cars_with_variant_info' => ['function' => 'fetch_latest_cars_data_from_db', 'args' => ['need_variant_info' => true]],
            'latest_cars' => ['function' => 'fetch_latest_cars_data_from_db', 'args' => []]
        ],
        'news' => [
            'single_thumbnail_news' => ['function' => 'fetch_single_thumbnail_news_data_from_db', 'args' => 'news'],
            'latest_news' => ['function' => 'fetch_latest_news_data_from_db', 'args' => 'news'],
            'category_news_' => ['function' => 'fetch_category_news_data', 'args' => ['category_id']],
            'subcategory_news_' => [], // '%prefix%' - 'prefix%'
            'latest_ev_news' => ['function' => 'fetch_latest_ev_news_data_from_db', 'args' => []],
            'ev_car_review_news_' => ['function' => 'fetch_ev_car_review_news_from_db', 'args' => []],
            'ev_car_comparison_news_' => ['function' => 'fetch_ev_car_comparison_news_from_db', 'args' => []],
            'ev_technology_news' => ['function' => 'fetch_ev_technology_news_from_db', 'args' => []],
            'car_news_data_' => ['function' => 'fetch_car_news_data_from_db', 'args' => []],
            'fuel_consumption_news' => ['function' => 'fetch_fuel_consumption_news_from_db', 'args' => []],
            'related_tag_news_' => ['function' => 'fetch_related_tag_news_data_from_db', 'args' => ['tag_id']],
        ],
        'faq' => [
            'car_faq_data_' => ['function' => 'fetch_car_faq_data_from_db', 'args' => []],
        ],
        'video' => [
            'latest_videos' => ['function' => 'fetch_latest_videos_from_db', 'args' => 'video'],
            'car_videos_data_' => ['function' => 'fetch_cars_videos_data_from_db', 'args' => []],
            'latest_ev_videos' => ['function' => 'fetch_latest_ev_videos_data_from_db', 'args' => []],
        ],
        'oil' => [
            'historical_oil_price_data' => ['function' => '', 'args' => []],
            'fuel_prices_home' => ['function' => 'fetch_fuel_price_data_from_db', 'args' => []],
            'petrol_and_diesel_data' => ['function' => 'fetch_petrol_and_diesel_data_from_db', 'args' => []],
        ],
        'listing' => [
            'ev_range_ranking' => ['function' => 'fetch_ev_range_ranking_data_from_db', 'args' => []],
            'ev_car_comparison' => ['function' => 'fetch_ev_car_comparison_data_from_db', 'args' => []],


            'brand_description_data_' => ['function' => 'fetch_brand_description_data_from_db', 'args' => []],
            'car_comparison_data_' => ['function' => 'fetch_car_comparison_data_from_db', 'args' => []],
            'grouped_by_type_cars_data_' => ['function' => 'fetch_grouped_by_type_cars_data_from_db', 'args' => []],

            'top_10_sedan_cars' => ['function' => 'fetch_top_10_sedan_data_from_db', 'args' => 'listing_make'],
            'top_car_models' => ['function' => 'fetch_top_car_models_from_db', 'args' => []],
            'all_top_car_model_ids' => ['function' => 'fetch_all_top_car_model_ids_from_db', 'args' => []],
            'popular_cars' => ['function' => 'fetch_popular_cars_data_from_db', 'args' => []],
            'recommended_cars' => ['function' => 'fetch_recommended_cars_from_db', 'args' => []],
            'popular_ev_cars' => ['function' => 'fetch_popular_ev_cars_data_from_db', 'args' => []],
        ],
        'make' => [
            'brands_list_home' => ['function' => 'fetch_brand_logo_data_from_db', 'args' => 'listing_make'],
            'brand_sidebar_data' => ['function' => 'fetch_brand_sidebar_data_from_db', 'args' => []],
            'ev_brands_list' => ['function' => 'fetch_brand_logo_data_from_db', 'args' => 'listing_make_evc'],
        ],
        'motorcycle-listing' => [
            'popular_bikes' => ['function' => 'fetch_popular_bikes_data_from_db', 'args' => []],
            'motor_brands_list_home' => ['function' => 'fetch_brand_logo_data_from_db', 'args' => 'motorcycle_make'],
            'latest_bikes' => ['function' => 'fetch_latest_bikes_data_from_db', 'args' => []],
            'grouped_by_type_motor_data_' => ['function' => 'fetch_grouped_by_type_motors_data_from_db', 'args' => []],
            'all_top_bike_model_ids' => ['function' => 'fetch_all_top_bike_model_ids_from_db', 'args' => []],
			'motor_comparison_data_' => ['function' => 'fetch_motor_comparison_data_from_db', 'args' => []],
			'motor_brand_description_data_' => ['function' => 'fetch_motor_brand_description_data_from_db', 'args' => []],
        ],
        'motorcycle-news' => [
            'motor_single_thumbnail_news' => ['function' => 'fetch_single_thumbnail_news_data_from_db', 'args' => 'motorcycle-news'],
            'motor_latest_news' => ['function' => 'fetch_latest_news_data_from_db', 'args' => 'motorcycle-news'],
            'motor_category_news_' => ['function' => 'fetch_motor_category_news_from_db', 'args' => ['category_id']],
            'motor_news_data_' => ['function' => 'fetch_motor_news_data_from_db', 'args' => []],
        ],
        'motocycle-video' => [
            'motor_latest_videos' => ['function' => 'fetch_latest_videos_from_db', 'args' => 'motocycle-video'],
            'motor_videos_data_' => ['function' => 'fetch_motor_videos_data_from_db', 'args' => []],
        ],
        'motorcycle-faq' => [
            'motor_faq_data_' => ['function' => 'fetch_motor_faq_data_from_db', 'args' => []],
        ],
        'other_keys' => []
    ];

    $post = get_post($post_id);
    if ($post_type === 'listing') {
        $post_meta = get_post_meta($post->ID);
        $listing_make_id = $post_meta['_listing_make'][0];
        $listing_make_term = get_term($listing_make_id);
        $make = $listing_make_term->slug;
        $model = $post->post_name;
        $model = str_replace($make . '-', '', $model);
        $cache_key = 'listing_post_' . $make . '_' . $model;
        delete_transient($cache_key);
    } elseif ($post_type === 'variant') {
        $listing_post_id = $post->post_parent;
        $listing_post = get_post($listing_post_id);
        $listing_post_meta = get_post_meta($listing_post_id);
        $listing_make_id = $listing_post_meta['_listing_make'][0];
        $listing_make_term = get_term($listing_make_id);
        $make = $listing_make_term->slug;
        $model = $listing_post->post_name;
        $model = str_replace($make . '-', '', $model);
        $variant_section = $post->post_name;
        $cache_key = 'variant_post_' . $make . '_' . $model . '_' . $variant_section;
        delete_transient($cache_key);
    }elseif ($post_type === 'motorcycle-listing') {
        $post_meta = get_post_meta($post->ID);
        $listing_make_id = $post_meta['make'][0];
        $listing_make_term = get_term($listing_make_id);
        $make = $listing_make_term->slug;
        $model = $post->post_name;
        $model = str_replace($make . '-', '', $model);
        $cache_key = 'motor_listing_post_' . $make . '_' . $model;
        delete_transient($cache_key);
    }

    // Return the cache keys for the specific post type, or an empty array if the post type doesn't exist
    return isset($cache_keys[$post_type]) ? $cache_keys[$post_type] : [];
}


//get cache key for term based on the taxanomy group
function get_cache_keys_for_term($term_id, $taxonomy)
{
    $cache_keys = [
        'listing_make' => [
            'brands_list_home' => ['function' => 'fetch_brand_logo_data_from_db', 'args' => 'listing_make'],
            'brand_sidebar_data' => ['function' => 'fetch_brand_sidebar_data_from_db', 'args' => []],
            'brand_description_data_' => ['function' => 'fetch_brand_description_data_from_db', 'args' => []],
            'car_comparison_data_' => ['function' => 'fetch_car_comparison_data_from_db', 'args' => []],
            'grouped_by_type_cars_data_' => ['function' => 'fetch_grouped_by_type_cars_data_from_db', 'args' => []],
        ],
        'listing_make_evc' => [
            'ev_brands_list' => ['function' => 'fetch_brand_logo_data_from_db', 'args' => 'listing_make_evc'],
        ],
        'motorcycle_make' => [
            'motor_brands_list_home' => ['function' => 'fetch_brand_logo_data_from_db', 'args' => 'motorcycle_make'],
            'motor_brand_sidebar_data' => ['function' => 'fetch_motor_brand_sidebar_data_from_db', 'args' => []],
            'grouped_by_type_motor_data_' => ['function' => 'fetch_grouped_by_type_motors_data_from_db', 'args' => []],
			'motor_comparison_data_' => ['function' => 'fetch_motor_comparison_data_from_db', 'args' => []],
			'motor_brand_description_data_' => ['function' => 'fetch_motor_brand_description_data_from_db', 'args' => []],
        ]
    ];

    if ($taxonomy === 'listing_make') {
        $term = get_term($term_id);
        $slug = '';
        if (!is_wp_error($term)) {
            $slug = $term->slug;
        }

        $cache_key = 'listing_post_' . $slug . '_';
        delete_transient($cache_key);

        $listing_cache_keys = get_cache_keys_grouped_by_post_type('listing');
        $make_cache_keys = $cache_keys['listing_make'];
        $all_keys = array_merge($listing_cache_keys, $make_cache_keys);
        return $all_keys;
    }elseif ($taxonomy === 'motorcycle_make') {
        $term = get_term($term_id);
        $slug = '';
        if (!is_wp_error($term)) {
            $slug = $term->slug;
        }

        $cache_key = 'motor_listing_post_' . $slug . '_';
        delete_transient($cache_key);

        $listing_cache_keys = get_cache_keys_grouped_by_post_type('motorcycle-listing');
        $make_cache_keys = $cache_keys['motorcycle_make'];
        $all_keys = array_merge($listing_cache_keys, $make_cache_keys);
        return $all_keys;
    } else {
        // Return the cache keys for the specific taxonomy, or an empty array if doesn't exist
        return isset($cache_keys[$taxonomy]) ? $cache_keys[$taxonomy] : [];
    }
}


//get cache key for option -> based on the page group
function get_cache_keys_for_option($page)
{
    $cache_keys = [
        'top-car-model' => [
            'top_10_sedan_cars' => ['function' => 'fetch_top_10_sedan_data_from_db', 'args' => 'listing_make'],
            'top_car_models' => ['function' => 'fetch_top_car_models_from_db', 'args' => []],
            'all_top_car_model_ids' => ['function' => 'fetch_all_top_car_model_ids_from_db', 'args' => []],
        ],
        'recommend-car-model' => [
            'popular_cars' => ['function' => 'fetch_popular_cars_data_from_db', 'args' => []],
            'recommended_cars' => ['function' => 'fetch_recommended_cars_from_db', 'args' => []],
            'popular_ev_cars' => ['function' => 'fetch_popular_ev_cars_data_from_db', 'args' => []],
        ],
        'recommend-bike-model' => [
            'popular_bikes' => ['function' => 'fetch_popular_bikes_data_from_db', 'args' => []],
            'latest_bikes' => ['function' => 'fetch_latest_bikes_data_from_db', 'args' => []],
        ],
        'top-bike-model' => [
            'all_top_bike_model_ids' => ['function' => 'fetch_all_top_bike_model_ids_from_db', 'args' => []],
        ],
    ];


    // Return the cache keys for the specific page, or an empty array if the post type doesn't exist
    return isset($cache_keys[$page]) ? $cache_keys[$page] : [];
}


/**
 * Clear and update cache based on post type.
 *
 * @param int $post_id The post ID being updated.
 * @param string $updated_post_type The updated post type.
 */
function handle_cache_update_on_post_type($post_id, $updated_post_type)
{
    if ($post_id > 0) {
        if (defined('DOING_AUTOSAVE') && DOING_AUTOSAVE) {
            return;
        }
        if (wp_is_post_revision($post_id)) {
            return;
        }
    }

    $cache_keys = get_cache_keys_grouped_by_post_type($updated_post_type, $post_id);
    // 	error_log(print_r($cache_keys, true));

    // If both `listing` and `variant` are updated, handle all their caches
    if (in_array($updated_post_type, ['listing', 'variant', 'motorcycle-listing'])) {
        clear_and_refresh_cache($cache_keys);
        return;
    }

    // Otherwise, handle caches specific to the post type
    if (!empty($cache_keys) && is_array($cache_keys)) {
        clear_and_refresh_cache($cache_keys);
    }
}


/**
 * Clear and refresh cache for given cache keys.
 *
 * @param int $post_id The ID of the post.
 * @param array $cache_keys The list of cache keys to clear and refresh.
 */
function clear_and_refresh_cache($cache_keys)
{
    foreach ($cache_keys as $cache_key => $cache_data) {
        // 		error_log('outside if........ ' . $cache_key);
        // If the cache key ends with an underscore, check for similar keys
        if (substr($cache_key, -1) === '_') {
            // Extract the prefix before the last underscore
            $prefix = rtrim($cache_key, '_');

            error_log('inside if.. ' . $cache_key);
            // Delete similar transient cache keys
            delete_dynamic_transients($prefix);
            delete_dynamic_transients_from_redis($prefix); // Match prefix
        }

        // Clear the cache
        delete_transient($cache_key);
        delete_redis_cache($cache_key);

        // Optional: Fetch fresh data if necessary
        // Call the function dynamically using the stored function name
        /*if ($cache_data['function']) {
            $fetch_function = $cache_data['function'];
            $args = $cache_data['args'];


            // Call the function and update cache with fresh data
            $data = function_exists($fetch_function) ? (!empty($args) ? $fetch_function($args) : $fetch_function()) : [];


            if (!empty($data)) {
                if (defined('USE_REDIS_CACHE') && USE_REDIS_CACHE) {
                    set_data_to_redis($cache_key, $data);
                }
                if (defined('USE_TRANSIENT_CACHE') && USE_TRANSIENT_CACHE) {
                    set_transient($cache_key, $data, defined('TRANSIENT_CACHE_TIMEOUT') ? TRANSIENT_CACHE_TIMEOUT : 3600);
                }
            }
        }*/
    }
}


function delete_dynamic_transients_from_redis($pattern)
{
    error_log('pattern in redis func..: ' . $pattern);
    $iterator = null;

    try {
        do {
            $all_redis_keys = get_all_redis_cache_keys();
            error_log(print_r($all_redis_keys, true));
            $keys = array_filter($all_redis_keys, function ($key) use ($pattern) {
                return strpos($key, $pattern) !== false;
            });

            error_log('filtered with pattern inside redis func');
            error_log(print_r($keys, true));

            if (!empty($keys)) {
                foreach ($keys as $key) {
                    delete_redis_cache($key);
                }
            }
        } while ($iterator > 0);
    } catch (Exception $e) {
        error_log('Error while deleting keys from Redis: ' . $e->getMessage());
    }
}


function delete_dynamic_transients($pattern)
{
    // Retrieve the list of all tracked transient keys
    global $wpdb;
    error_log($pattern);
    $like_key = '_transient_' . $pattern . '%';
    $query = "SELECT * FROM {$wpdb->prefix}options WHERE option_name LIKE '$like_key'";
    $transient_keys = $wpdb->get_results($query);
    error_log('transient keys........');
    error_log(print_r($transient_keys, true));

    set_transient('test_transient', 'hello world', 3600);
    error_log(get_transient('test_transient'));

    if (!empty($transient_keys)) {
        foreach ($transient_keys as $transient_key) {
            $transient_key = $transient_key->option_name;

            $transient_name = str_replace('_transient_', '', $transient_key);
            delete_transient($transient_name);
        }
    }
}


//trigger when term is added, updated, or deleted
function handle_cache_update_on_taxonomy($term_id, $taxonomy)
{
    // Check the taxonomy and handle cache clearing accordingly
    if ($taxonomy) {
        // You can define cache clearing logic specific to terms and taxonomies here
        // For example:
        $cache_keys = get_cache_keys_for_term($term_id, $taxonomy);

        clear_and_refresh_cache($cache_keys);
    }
}


function handle_cache_update_on_option($page)
{
    // Check if the option name corresponds to any specific cacheable options
    if ($page) {
        // Define cache keys to clear when a specific option changes or is deleted
        $cache_keys = get_cache_keys_for_option($page);
        clear_and_refresh_cache($cache_keys);
    }
}


// Hook into save post to handle cache clearing
add_action('save_post', function ($post_id) {
    // Avoid autosave and revisions
    if (defined('DOING_AUTOSAVE') && DOING_AUTOSAVE) return;
    if (wp_is_post_revision($post_id)) return;

    // Only trigger for the relevant post type
    $post_type = get_post_type($post_id);
    if ($post_type) {
        handle_cache_update_on_post_type($post_id, $post_type);
    }
});


// Hook into update post to handle cache clearing
add_action('updated_post_meta', function ($post_id) {
    $post_type = get_post_type($post_id);
    if ($post_type) {
        handle_cache_update_on_post_type($post_id, $post_type);
    }
});


// Hook into delete post to handle cache clearing
add_action('deleted_post_meta', function ($post_id) {
    $post_type = get_post_type($post_id);
    if ($post_type) {
        handle_cache_update_on_post_type($post_id, $post_type);
    }
});


// Hook into term creation, editing, or deletion to handle cache clearing
add_action('created_term', function ($term_id, $tt_id, $taxonomy) {
    handle_cache_update_on_taxonomy($term_id, $taxonomy);
}, 10, 3);


add_action('edited_term', function ($term_id, $tt_id) {
    $term = get_term($term_id);
    // Debugging term ID and taxonomy
    if (!is_wp_error($term) && $term) {
        // Use the term's taxonomy property instead of relying on the parameter
        $taxonomy_name = $term->taxonomy;
    } else {
        error_log("Error occurred while fetching term.");
    }

    handle_cache_update_on_taxonomy($term_id, $taxonomy_name);
}, 10, 2);


add_action('pre_delete_term', function ($term_id, $taxonomy) {
    handle_cache_update_on_taxonomy($term_id, $taxonomy);
}, 10, 2);




//hook into update option, to handle cache clearing
// Hook into option updates, deletions to handle cache clearing
add_action('added_option', function ($option_name) {
    $page = isset($_GET['page']) ? sanitize_text_field($_GET['page']) : '';
    handle_cache_update_on_option($page);
});
add_action('updated_option', function ($option_name) {
    $page = isset($_GET['page']) ? sanitize_text_field($_GET['page']) : '';
    handle_cache_update_on_option($page);
});


add_action('deleted_option', function ($option_name) {
    $page = isset($_GET['page']) ? sanitize_text_field($_GET['page']) : '';
    handle_cache_update_on_option($page);
});
