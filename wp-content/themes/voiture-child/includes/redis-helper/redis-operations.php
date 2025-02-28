<?php

global $redis;

// Check if Redis Object Cache plugin is available
if (function_exists('redis_object_cache')) {
    // Use the plugin's Redis connection
    $redis = redis_object_cache();
} elseif (class_exists('Redis')) {
    // Initialize Redis connection directly if the plugin is not available
    try {
        $redis = new Redis();
        $redis->connect(WP_REDIS_HOST, WP_REDIS_PORT);
        error_log('Connected to Redis successfully via PHP Redis');
    } catch (Exception $e) {
        error_log('Redis connection failed: ' . $e->getMessage());
        $redis = null;  // Fallback if connection fails
    }
} else {
    error_log('Redis plugin or class is not available.');
}

/**
 * Fetch data from Redis cache using wp_cache_get.
 *
 * @param string $key Cache key.
 * @return mixed|false Cached data or false if not found.
 */
function get_data_from_redis($key)
{
    return wp_cache_get($key); // Directly use wp_cache_get for retrieving cached data
}

/**
 * Set data to Redis cache using wp_cache_set with a specified group.
 *
 * @param string $key Cache key.
 * @param mixed $data Data to cache.
 * @param int $timeout Cache expiration time in seconds.
 * @param string $group Cache group (optional).
 */
function set_data_to_redis($key, $data, $timeout = 3600, $group = '')
{
    // If no group is specified, default to the empty string
    $group = !empty($group) ? $group : '';

    // Store data in Redis cache using the provided group
    wp_cache_set($key, $data, $group, $timeout);


    // store cache keys with all_cache_keys as key
    $cache_keys = wp_cache_get('all_cache_keys') ?: [];
    if (!in_array($key, $cache_keys)) {
        $cache_keys[] = $key;
        wp_cache_set('all_cache_keys', $cache_keys, '', 0); // No expiration
    }
}

/**
 * Clear specific Redis cache by key.
 *
 * @param string $cache_key Cache key to delete.
 */
function delete_redis_cache($cache_key)
{
    wp_cache_delete($cache_key); // Use wp_cache_delete to remove cached data
}

/**
 * Clear specific Transient cache by key.
 *
 * @param string $cache_key Cache key to delete.
 */
function delete_transient_cache($cache_key)
{
    delete_transient($cache_key); // Remove transient cache
}



function get_all_redis_cache_keys()
{
    $keys = wp_cache_get('all_cache_keys');

    return $keys ?: [];
}
