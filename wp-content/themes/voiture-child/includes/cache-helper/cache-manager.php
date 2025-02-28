<?php

/**
 * Generic function to fetch and cache data with support for Redis, Transients, or database fallback.
 *
 * @param string   $cache_key      The cache key to store/retrieve data.
 * @param callable $fetch_callback Callback function to fetch data from the source (e.g., database).
 * @param int      $cache_timeout  Cache expiration time in seconds.
 * @return mixed The fetched or cached data.
 */
function fetch_and_cache_data($cache_key, callable $fetch_callback, $cache_timeout = 3600, $callback_args = '')
{

    $use_redis_cache = defined('USE_REDIS_CACHE') && USE_REDIS_CACHE;
    $use_transient_cache = defined('USE_TRANSIENT_CACHE') && USE_TRANSIENT_CACHE;

    // Use passed cache timeout if provided, otherwise determine dynamically
    if ($cache_timeout === 3600) {
        if (defined('REDIS_CACHE_TIMEOUT') && $use_redis_cache) {
            $cache_timeout = REDIS_CACHE_TIMEOUT;
        } elseif (defined('TRANSIENT_CACHE_TIMEOUT') && $use_transient_cache) {
            $cache_timeout = TRANSIENT_CACHE_TIMEOUT;
        }
    }

    // Check if data should be fetched directly from the database
    if (FETCH_FROM_DB) {
        // Fetch directly from the database
        $data = $callback_args
            ? call_user_func($fetch_callback, $callback_args)
            : call_user_func($fetch_callback);

        // Store the data in Redis and transients
        set_data_to_redis($cache_key, $data);
        set_transient($cache_key, $data, $cache_timeout);

        // Return the data fetched from the database
        return $data;
    }

    // Attempt to fetch the data from Redis cache
    if ($use_redis_cache) {
        $data = get_data_from_redis($cache_key);
    }

    // Fallback to Transient cache if Redis is unavailable or disabled
    if (($data === false || $data === null || $data === '') && $use_transient_cache) {
        $data = get_transient($cache_key);
    }

    // As a last resort, fetch data directly from the database if not in any cache
    if ($data === false || $data === null || $data === '') {
        $data = function_exists($fetch_callback)
            ? (!empty($callback_args) ? $fetch_callback($callback_args) : $fetch_callback())
            : array();

        // Update both Redis and transient caches with fresh data
        set_data_to_redis($cache_key, $data);
        set_transient($cache_key, $data, TRANSIENT_CACHE_TIMEOUT);
    }

    // Return the fetched or cached carousel data
    return $data;
}

/**
 * Fetch carousel data with caching using `fetch_and_cache_data`.
 *
 * @return array The carousel data.
 */
function get_carousel_data()
{
    $cache_key = 'banner_carousel';
    return fetch_and_cache_data($cache_key, 'fetch_carousel_data_from_db');
}

function  get_top_10_sedan_data()
{
    $cache_key = 'top_10_sedan_cars';
    return fetch_and_cache_data($cache_key, 'fetch_top_10_sedan_data_from_db');
}

function  get_popular_cars_data($need_variant_info = false)
{
    $cache_key = 'popular_cars';
    if ($need_variant_info) {
        $cache_key = 'popular_cars_with_variant_info';
    }
    return fetch_and_cache_data($cache_key, 'fetch_popular_cars_data_from_db', 3600, ['need_variant_info' => $need_variant_info]);
}

function  get_latest_cars_data($need_variant_info = false)
{
    $cache_key = 'latest_cars';
    if ($need_variant_info) {
        $cache_key = 'latest_cars_with_variant_info';
    }
    return fetch_and_cache_data($cache_key, 'fetch_latest_cars_data_from_db', 3600, ['need_variant_info' => $need_variant_info]);
}

function  get_recommended_cars_data($need_variant_info = false)
{
    $cache_key = 'recommended_cars';
    if ($need_variant_info) {
        $cache_key = 'recommended_cars_with_variant_info';
    }

    return fetch_and_cache_data($cache_key, 'fetch_recommended_cars_from_db', 3600, ['need_variant_info' => $need_variant_info]);
}

function  get_top_car_models_data($need_variant_info = false)
{
    $cache_key = 'top_car_models';
    if ($need_variant_info) {
        $cache_key = 'top_car_models_with_variant_info';
    }

    return fetch_and_cache_data($cache_key, 'fetch_top_car_models_from_db', 3600, ['need_variant_info' => $need_variant_info]);
}

function  get_all_top_car_model_ids_data()
{
    $cache_key = 'all_top_car_model_ids';

    return fetch_and_cache_data($cache_key, 'fetch_all_top_car_model_ids_from_db');
}

function get_brand_list_data($taxonomy_type)
{
    $cache_key = 'brands_list_home';

    return fetch_and_cache_data($cache_key, 'fetch_brand_logo_data_from_db', '', $taxonomy_type);
}

function get_single_thumbnail_news_data($post_type)
{
    $cache_key = 'single_thumbnail_news';

    return fetch_and_cache_data($cache_key, 'fetch_single_thumbnail_news_data_from_db', 3600, ['post_type' => $post_type]);
}

function get_latest_news_data($post_type)
{
    $cache_key = 'latest_news';

    return fetch_and_cache_data($cache_key, 'fetch_latest_news_data_from_db', 3600, ['post_type' => $post_type]);
}

function  get_latest_videos_data($post_type)
{
    $cache_key = 'latest_videos';

    return fetch_and_cache_data($cache_key, 'fetch_latest_videos_from_db', 3600, ['post_type' => $post_type]);
}

function  get_fuel_price_data()
{
    $cache_key = 'fuel_prices_home';
    return fetch_and_cache_data($cache_key, 'fetch_fuel_price_data_from_db');
}

function get_category_news_data($category, $description = 'sub-category', $current_page = 1)
{
    $second_language = get_current_language();
    $cache_key = 'category_news_' . $category . '_' . $description . '_' . $current_page . '_' . $second_language;

    return fetch_and_cache_data(
        $cache_key,
        'fetch_category_news_from_db',
        3600,
        ['category' => $category, 'description' => $description, 'paged' => $current_page]
    );
}

function get_related_tag_news_data($tag, $current_page = 1)
{
    $cache_key = 'related_tag_news_' . $tag . '_' . $current_page;

    return fetch_and_cache_data($cache_key, 'fetch_related_tag_news_data_from_db', 3600, ['tag' => $tag, 'paged' => $current_page]);
}

function get_brand_list_ev_data($taxonomy_type)
{
    $cache_key = 'ev_brands_list';
    return fetch_and_cache_data($cache_key, 'fetch_brand_logo_data_from_db', '', $taxonomy_type);
}

function  get_popular_ev_cars_data()
{
    $cache_key = 'popular_ev_cars';
    return fetch_and_cache_data($cache_key, 'fetch_popular_ev_cars_data_from_db');
}

function  get_latest_ev_news_data()
{
    $cache_key = 'latest_ev_news';
    return fetch_and_cache_data($cache_key, 'fetch_latest_ev_news_data_from_db');
}

function  get_ev_range_ranking_data()
{
    $cache_key = 'ev_range_ranking';
    return fetch_and_cache_data($cache_key, 'fetch_ev_range_ranking_data_from_db');
}

// category news
function get_ev_car_review_news_data($current_page)
{
    $cache_key = 'ev_car_review_news_' . $current_page;
    return fetch_and_cache_data($cache_key, 'fetch_ev_car_review_news_from_db', 3600, ['paged' => $current_page]);
}

function get_ev_car_comparison_news_data($current_page)
{
    $cache_key = 'ev_car_comparison_news_' . $current_page;
    return fetch_and_cache_data($cache_key, 'fetch_ev_car_comparison_news_from_db', 3600, ['paged' => $current_page]);
}

function  get_ev_car_comparison_data()
{
    $cache_key = 'ev_car_comparison';
    return fetch_and_cache_data($cache_key, 'fetch_ev_car_comparison_data_from_db');
}

function  get_latest_ev_videos_data()
{
    $cache_key = 'latest_ev_videos';
    return fetch_and_cache_data($cache_key, 'fetch_latest_ev_videos_data_from_db');
}

function  get_ev_technology_news_data()
{
    $cache_key = 'ev_technology_news';
    return fetch_and_cache_data($cache_key, 'fetch_ev_technology_news_from_db');
}

// new cars page

// brand sidebar data
function  get_brand_sidebar_data()
{
    $cache_key = 'brand_sidebar_data';

    return fetch_and_cache_data($cache_key, 'fetch_brand_sidebar_data_from_db');
}

// brand description
function  get_brand_description_data($brand_id)
{
    $cache_key = 'brand_description_data_' . $brand_id;

    return fetch_and_cache_data($cache_key, 'fetch_brand_description_data_from_db');
}

// car comparison
function  get_car_comparison_data($make)
{
    $cache_key = 'car_comparison_data_' . $make;

    return fetch_and_cache_data($cache_key, 'fetch_car_comparison_data_from_db');
}

// grouped by type cars
function  get_grouped_by_type_cars_data($make)
{
    $cache_key = 'grouped_by_type_cars_data_' . $make;

    return fetch_and_cache_data($cache_key, 'fetch_grouped_by_type_cars_data_from_db');
}

// car news
function  get_car_news_data($brand_id)
{
    $cache_key = 'car_news_data_' . $brand_id;

    return fetch_and_cache_data($cache_key, 'fetch_car_news_data_from_db');
}

// car videos
function  get_car_videos_data($brand_id = '')
{
    $cache_key = 'car_videos_data_' . $brand_id;

    return fetch_and_cache_data($cache_key, 'fetch_cars_videos_data_from_db');
}

// car faq
function  get_car_faq_data($brand_name)
{
    $cache_key = 'car_faq_data_' . $brand_name;

    return fetch_and_cache_data($cache_key, 'fetch_car_faq_data_from_db');
}

// oil page
function get_petrol_and_diesel_data()
{
    $cache_key = 'petrol_and_diesel_data';

    return fetch_and_cache_data($cache_key, 'fetch_petrol_and_diesel_data_from_db');
}

function get_fuel_consumption_news_data()
{
    $cache_key = 'fuel_consumption_news';

    return fetch_and_cache_data($cache_key, 'fetch_fuel_consumption_news_from_db');
}

function get_historical_oil_price_data($args)
{
    $cache_key = 'historical_oil_price_data';

    return fetch_and_cache_data($cache_key, 'fetch_historical_oil_price_data_from_db', 3600, $args);
}


//motorcycle cache keys
function get_motor_brand_list_data($taxonomy_type)
{
    $cache_key = 'motor_brands_list_home';

    return fetch_and_cache_data($cache_key, 'fetch_brand_logo_data_from_db', '', $taxonomy_type);
}

function get_popular_bikes_data($need_variant_info = false)
{
    $cache_key = 'popular_bikes';
    if ($need_variant_info) {
        $cache_key = 'popular_bikes_with_variant_info';
    }
    return fetch_and_cache_data($cache_key, 'fetch_popular_bikes_data_from_db', 3600, ['need_variant_info' => $need_variant_info]);
}

function get_latest_bikes_data($need_variant_info = false)
{
    $cache_key = 'latest_bikes';
    if ($need_variant_info) {
        $cache_key = 'latest_bikes_with_variant_info';
    }
    return fetch_and_cache_data($cache_key, 'fetch_latest_bikes_data_from_db', 3600, ['need_variant_info' => $need_variant_info]);
}

function get_motor_single_thumbnail_news_data($post_type)
{
    $cache_key = 'motor_single_thumbnail_news';

    return fetch_and_cache_data($cache_key, 'fetch_single_thumbnail_news_data_from_db', 3600, ['post_type' => $post_type]);
}

function get_motor_latest_videos_data($post_type)
{
    $cache_key = 'motor_latest_videos';

    return fetch_and_cache_data($cache_key, 'fetch_latest_videos_from_db', 3600, ['post_type' => $post_type]);
}

function get_motor_latest_news_data($post_type)
{
    $cache_key = 'motor_latest_news';

    return fetch_and_cache_data($cache_key, 'fetch_latest_news_data_from_db', 3600, ['post_type' => $post_type]);
}

function get_motor_category_news_data($category, $description = 'sub-category', $current_page = 1)
{
    $cache_key = 'motor_category_news_' . $category . '_' . $description . '_' . $current_page;

    return fetch_and_cache_data(
        $cache_key,
        'fetch_motor_category_news_from_db',
        3600,
        ['category' => $category, 'description' => $description, 'paged' => $current_page]
    );
}

function  get_motor_brand_sidebar_data()
{
    $cache_key = 'motor_brand_sidebar_data';

    return fetch_and_cache_data($cache_key, 'fetch_motor_brand_sidebar_data_from_db');
}

// grouped by type motors
function  get_grouped_by_type_motors_data($make)
{
    $cache_key = 'grouped_by_type_motor_data_' . $make;

    return fetch_and_cache_data($cache_key, 'fetch_grouped_by_type_motors_data_from_db');
}

function  get_all_top_bike_model_ids_data()
{
    $cache_key = 'all_top_bike_model_ids';

    return fetch_and_cache_data($cache_key, 'fetch_all_top_bike_model_ids_from_db');
}

// motor videos
function  get_motor_videos_data($brand_id = '')
{
    $cache_key = 'motor_videos_data_' . $brand_id;

    return fetch_and_cache_data($cache_key, 'fetch_motor_videos_data_from_db');
}

// motor news
function  get_motor_news_data($brand_id)
{
    $cache_key = 'motor_news_data_' . $brand_id;

    return fetch_and_cache_data($cache_key, 'fetch_motor_news_data_from_db');
}

// motor faq
function  get_motor_faq_data($brand_name)
{
    $cache_key = 'motor_faq_data_' . $brand_name;

    return fetch_and_cache_data($cache_key, 'fetch_motor_faq_data_from_db');
}

// motor comparison
function  get_motor_comparison_data($make)
{
    $cache_key = 'motor_comparison_data_' . $make;

    return fetch_and_cache_data($cache_key, 'fetch_motor_comparison_data_from_db');
}

// motor brand description
function  get_motor_brand_description_data($brand_id)
{
    $cache_key = 'motor_brand_description_data_' . $brand_id;

    return fetch_and_cache_data($cache_key, 'fetch_motor_brand_description_data_from_db');
}