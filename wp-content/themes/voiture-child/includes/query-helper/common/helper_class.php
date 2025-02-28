<?php

/**
 * Helper function to batch load meta data for multiple posts.
 */
function get_post_meta_for_posts($post_ids, $batch_size = 100)
{
    global $wpdb;

    if (empty($post_ids)) {
        return [];
    }

    // Sort post IDs to improve partitioned table performance
    sort($post_ids);

    $meta = [];

    foreach (array_chunk($post_ids, $batch_size) as $batch) {
        $placeholders = implode(',', array_fill(0, count($batch), '%d'));
        $query = $wpdb->prepare(
            "SELECT post_id, meta_key, meta_value 
             FROM $wpdb->postmeta 
             WHERE post_id IN ($placeholders)
             ORDER BY post_id, meta_key",
            $batch
        );

        $results = $wpdb->get_results($query, ARRAY_A);

        foreach ($results as $row) {
            $meta[$row['post_id']][$row['meta_key']][] = $row['meta_value'];
        }
    }

    return $meta;
}
/**
 * Get the price range of a listing based on its child variants.
 *
 * This function calculates the lowest and highest prices among the variants
 * associated with a given listing post. It utilizes batch meta fetching via
 * `get_post_meta_for_posts` for better performance.
 *
 * @param int $listing_post_id The ID of the listing post.
 *
 * @return string The price range as a formatted string (e.g., "RM 20,000 - RM 30,000"),
 *                or "TBC" if no valid prices are available.
 *
 * Optimization Highlights:
 * - Uses `get_posts` with `'fields' => 'ids'` to minimize data load when fetching variants.
 * - Leverages `get_post_meta_for_posts` to batch fetch meta data, reducing database queries.
 * - Handles edge cases where no variants or prices are available.
 */
function get_price_range_of_listing($listing_post_id)
{
    global $wpdb;

    $prices = $wpdb->get_col($wpdb->prepare("
		SELECT meta_value 
		FROM {$wpdb->postmeta} pm
		INNER JOIN {$wpdb->posts} p ON p.ID = pm.post_id
		WHERE p.post_parent = %d 
		AND p.post_type = 'variant' 
		AND pm.meta_key = 'retail_price'
		AND pm.meta_value > 0
	", $listing_post_id));

    if (empty($prices)) {
        return 'ยังไม่คอนเฟิร์ม';
    }

    // Convert to float and find min/max
    $prices = array_map('floatval', $prices);
    $lowest_price = min($prices);
    $highest_price = max($prices);

    if ($lowest_price === $highest_price) {
        return 'THB ' . format_number_with_commas($highest_price);
    }

    return 'THB ' . format_number_with_commas($lowest_price) . ' - THB ' . format_number_with_commas($highest_price);
}

/**
 * Fetches all post metadata for the given post IDs and adds the GUID for the thumbnail image 
 * (if available) to the metadata.
 *
 * This function:
 * 1. Retrieves all post meta for the specified post IDs.
 * 2. Collects all unique `_thumbnail_id` values from the post meta.
 * 3. Fetches the `guid` values for all unique `_thumbnail_id`s in a single database query.
 * 4. Adds the corresponding `guid` to the post metadata for each post that has a `_thumbnail_id`.
 *
 * @param array $post_ids The array of post IDs for which metadata needs to be retrieved.
 * @return array An associative array where each key is a post ID, and the value is an array of post meta data
 *               (including the '_thumbnail_guid' if the post has a thumbnail).
 */
function get_post_meta_with_thumbnail_guid($post_ids, $batch_size = 100)
{
    global $wpdb;

    if (empty($post_ids)) {
        return [];
    }

    // Sort post IDs to optimize partitioned queries
    sort($post_ids);

    $meta = [];
    $thumbnail_ids = [];

    // Step 1: Fetch post meta in batches
    foreach (array_chunk($post_ids, $batch_size) as $batch) {
        $placeholders = implode(',', array_fill(0, count($batch), '%d'));
        $query = $wpdb->prepare(
            "SELECT post_id, meta_key, meta_value 
             FROM $wpdb->postmeta 
             WHERE post_id IN ($placeholders) 
             AND meta_key IN ('_thumbnail_id', 'listing-state', '_listing_make', 'publish_time')",
            $batch
        );

        $results = $wpdb->get_results($query, ARRAY_A);

        foreach ($results as $row) {
            $meta[$row['post_id']][$row['meta_key']][] = $row['meta_value'];

            // Collect unique thumbnail IDs
            if ($row['meta_key'] === '_thumbnail_id') {
                $thumbnail_ids[] = (int) $row['meta_value'];
            }
        }
    }

    // Step 2: Fetch thumbnail GUIDs
    $thumbnail_ids = array_unique($thumbnail_ids);
    if (!empty($thumbnail_ids)) {
        sort($thumbnail_ids); // Optimized for partitioning

        foreach (array_chunk($thumbnail_ids, $batch_size) as $batch) {
            $placeholders = implode(',', array_fill(0, count($batch), '%d'));
            $guid_query = $wpdb->prepare(
                "SELECT ID, guid FROM $wpdb->posts WHERE ID IN ($placeholders)",
                $batch
            );

            $guid_results = $wpdb->get_results($guid_query, ARRAY_A);

            $thumbnail_guids = [];
            foreach ($guid_results as $guid_row) {
                $thumbnail_guids[$guid_row['ID']] = $guid_row['guid'];
            }

            // Step 3: Attach thumbnail GUIDs to post meta
            foreach ($meta as $post_id => $meta_values) {
                if (!empty($meta_values['_thumbnail_id'])) {
                    $thumb_id = (int) $meta_values['_thumbnail_id'][0];
                    if (isset($thumbnail_guids[$thumb_id])) {
                        $meta[$post_id]['_thumbnail_guid'] = $thumbnail_guids[$thumb_id];
                    }
                }
            }
        }
    }

    return $meta;
}


/**
 * Retrieves the thumbnail URLs for a given list of post IDs.
 *
 * This function queries the WordPress database to fetch the thumbnail
 * (featured image) IDs for the given posts and then retrieves their URLs.
 *
 * @param array $post_ids An array of post IDs.
 * @return array Associative array of post IDs mapped to their thumbnail URLs.
 */
function get_post_thumbnail_urls($post_ids, $batch_size = 100)
{
    global $wpdb;

    if (empty($post_ids)) {
        return [];
    }

    // Sort post IDs to optimize partition access
    sort($post_ids);

    $thumbnail_urls = [];

    // Step 1: Fetch _thumbnail_id in batches
    foreach (array_chunk($post_ids, $batch_size) as $batch) {
        $placeholders = implode(',', array_fill(0, count($batch), '%d'));
        $query = $wpdb->prepare(
            "SELECT post_id, meta_value 
             FROM $wpdb->postmeta 
             WHERE post_id IN ($placeholders) AND meta_key = '_thumbnail_id'",
            $batch
        );

        $results = $wpdb->get_results($query, ARRAY_A);

        $thumbnail_ids_map = [];
        foreach ($results as $row) {
            $thumbnail_ids_map[$row['post_id']] = (int) $row['meta_value'];
        }

        // Step 2: Fetch guid from wp_posts in batches
        $thumbnail_ids = array_values($thumbnail_ids_map);
        if (!empty($thumbnail_ids)) {
            sort($thumbnail_ids); // Optimized for partition pruning

            foreach (array_chunk($thumbnail_ids, $batch_size) as $thumb_batch) {
                $placeholders = implode(',', array_fill(0, count($thumb_batch), '%d'));
                $guid_query = $wpdb->prepare(
                    "SELECT ID, guid FROM $wpdb->posts WHERE ID IN ($placeholders)",
                    $thumb_batch
                );

                $guid_results = $wpdb->get_results($guid_query, ARRAY_A);

                // Step 3: Map thumbnail IDs to their GUIDs
                $thumbnail_guids = [];
                foreach ($guid_results as $guid_row) {
                    $thumbnail_guids[$guid_row['ID']] = $guid_row['guid'];
                }

                // Step 4: Assign URLs back to post IDs
                foreach ($thumbnail_ids_map as $post_id => $thumb_id) {
                    if (isset($thumbnail_guids[$thumb_id])) {
                        $thumbnail_urls[$post_id] = $thumbnail_guids[$thumb_id];
                    }
                }
            }
        }
    }

    return $thumbnail_urls;
}

function get_selected_meta_data_for_posts($post_ids, $meta_keys, $batch_size = 100)
{
    global $wpdb;

    if (empty($post_ids) || empty($meta_keys)) {
        return [];
    }

    // Sort for partition efficiency
    sort($post_ids);
    sort($meta_keys);

    $meta = [];

    foreach (array_chunk($post_ids, $batch_size) as $batch) {
        $placeholders = implode(',', array_fill(0, count($batch), '%d'));
        $query = $wpdb->prepare(
            "SELECT post_id, meta_key, meta_value 
             FROM $wpdb->postmeta 
             WHERE post_id IN ($placeholders) 
             AND meta_key IN (" . implode(',', array_fill(0, count($meta_keys), '%s')) . ")",
            array_merge($batch, $meta_keys)
        );

        $results = $wpdb->get_results($query, ARRAY_A);

        foreach ($results as $row) {
            $meta[$row['post_id']][$row['meta_key']][] = $row['meta_value'];
        }
    }

    return $meta;
}

function get_author_data($post_author_ids)
{
    global $wpdb;

    if (empty($post_author_ids)) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($post_author_ids), '%d'));
    $author_query = $wpdb->prepare(
        "SELECT ID, display_name 
         FROM $wpdb->users 
         WHERE ID IN ($placeholders)",
        $post_author_ids
    );

    $author_results = $wpdb->get_results($author_query, ARRAY_A);

    $authors = [];
    foreach ($author_results as $author_row) {
        $authors[$author_row['ID']] = [
            'display_name' => $author_row['display_name'],
            'author_image_url' => 'https://storage.googleapis.com/wp-my/malaysia/2025/01/13003120/author-placeholder.jpg',
        ];
    }

    // get image url of author from user meta
    $placeholders = implode(',', array_fill(0, count($post_author_ids), '%d'));
    $author_images_query = $wpdb->prepare(
        "SELECT user_id, meta_value 
         FROM $wpdb->usermeta 
         WHERE user_id IN ($placeholders) AND meta_key = 'author_image'",
        $post_author_ids
    );
    $author_image_results = $wpdb->get_results($author_images_query, ARRAY_A);

    foreach ($author_image_results as $author_image_row) {
        $author_id = $author_image_row['user_id'];
        $author_image_url = $author_image_row['meta_value'];
        $authors[$author_id]['author_image_url'] = $author_image_url;
    }

    return $authors;
}

/**
 * Formats car listing responses based on given car IDs.
 *
 * This function retrieves car listings from the database, extracts relevant 
 * information such as title, thumbnail, price range, and permalink, and 
 * returns them in a structured array.
 *
 * @param array $car_ids Array of car listing IDs to retrieve.
 * @return array Structured array containing car listing details.
 */
function format_car_response($car_ids, $need_variant_info = false)
{
    if (empty($car_ids)) {
        return [];
    }

    $posts = get_posts([
        'post_type'      => 'listing',
        'posts_per_page' => count($car_ids),
        'post__in'       => $car_ids,
        'orderby'        => 'post__in',
        'fields'         => 'all', // Fetch all necessary fields
    ]);

    return format_car_response_by_posts($posts, $need_variant_info);
}

function format_car_response_by_posts($posts, $need_variant_info = false)
{
    if (empty($posts)) {
        return [];
    }

    $cars = [];
    $post_ids = wp_list_pluck($posts, 'ID');

    // Fetch required data in bulk
    $required_meta = get_post_meta_with_thumbnail_guid($post_ids);

    foreach ($posts as $post) {
        $car_id = $post->ID;
        $thumbnail_url = $required_meta[$car_id]['_thumbnail_guid'] ??  CAR_PLACEHOLDER;
        $price_range = get_price_range_of_listing($car_id);
        $listing_state = $required_meta[$car_id]['listing-state'][0] ?? '';
        $listing_make_id = $required_meta[$car_id]['_listing_make'][0] ?? '';

        // Fetch term name safely
        $listing_make = '';
        if (!empty($listing_make_id)) {
            $term = get_term($listing_make_id);
            if (!is_wp_error($term) && !empty($term->name)) {
                $listing_make = $term->name;
            }
        }

        $car_data = [
            'id'             => $car_id,
            'post_title'     => $post->post_title,
            'thumbnail_url'  => $thumbnail_url,
            'price_range'    => $price_range,
            'permalink'      => get_permalink($car_id),
            'post_name'      => $post->post_name,
            'listing_state'  => $listing_state,
            'listing_make'   => $listing_make,
            'variants'       => [],
        ];

        if ($need_variant_info) {
            $car_data['variants'] = get_variant_info($car_id);
        }

        $cars[] = $car_data;
    }

    return $cars;
}

function get_variant_info($listing_post_id)
{
    $args = array(
        'post_type' => 'variant',
        'posts_per_page' => 5,
        'post_parent' => $listing_post_id,
    );
    $variant_posts = get_posts($args);

    $variants = [];
    foreach ($variant_posts as $variant_post) {
        $variant_id = $variant_post->ID;
        $variant_title = $variant_post->post_title;
        $variant_post_name = $variant_post->post_name;
        $variants[] = [
            'id' => $variant_id,
            'title' => $variant_title,
            'post_name' => $variant_post_name,
        ];
    }

    return $variants;
}


// Generic function to fetch multiple fields for multiple terms
// Generic function to fetch multiple fields for multiple terms
function fetch_terms_data($term_ids, $fields)
{
    global $wpdb;

    // Sanitize input
    $term_ids = array_map('intval', $term_ids);
    $fields = array_map('sanitize_text_field', $fields);

    if (empty($term_ids) || empty($fields)) {
        return []; // Return an empty array if no term IDs or fields are provided
    }

    // Start building the query
    $term_placeholders = implode(', ', array_fill(0, count($term_ids), '%d')); // %d for integers (term_ids)
    $field_selects = [];
    $field_placeholders = [];

    // Create field select statements for each field and its placeholder
    foreach ($fields as $field) {
        $field_selects[] = "MAX(CASE WHEN meta_key = %s THEN meta_value END) AS {$field}";
        $field_placeholders[] = $field; // Add the field name (for meta_key) as placeholder
    }

    // Build the dynamic SQL query
    $sql = "
        SELECT t.term_id, t.slug, " . implode(', ', $field_selects) . "
        FROM {$wpdb->terms} t
        LEFT JOIN {$wpdb->termmeta} tm ON t.term_id = tm.term_id
        WHERE t.term_id IN ($term_placeholders)
        GROUP BY t.term_id
    ";

    // Merge field placeholders and term_ids
    $query_params = array_merge($field_placeholders, $term_ids);

    // Run the query
    $results = $wpdb->get_results($wpdb->prepare($sql, ...$query_params), ARRAY_A);
    return $results;
}

function formatted_news_data($news_posts)
{
    $news_data = [];
    if (!empty($news_posts)) {
        $post_ids = wp_list_pluck($news_posts, 'ID');
        $all_meta = get_post_meta_with_thumbnail_guid($post_ids);
        foreach ($news_posts as $news_post) {
            $description = wp_trim_words($news_post->post_content, 20, '...');
            $news_href = get_custom_post_link($news_post->ID, '');
            $thumbnail_url = $all_meta[$news_post->ID]['_thumbnail_guid'] ?? CAR_PLACEHOLDER;

            $publish_time = $all_meta[$news_post->ID]['publish_time'][0] ?? '';
            $publish_time = date('d.m.Y', strtotime($publish_time));

            $author_id = $news_post->post_author;
            $author_name = get_the_author_meta('display_name', $author_id);

            $news_data[] = [
                'id'       => $news_post->ID,
                'title'    => $news_post->post_title,
                'guid'   => $thumbnail_url,
                'description' => $description,
                'news_href' => $news_href,
                'publish_time' => $publish_time,
                'author' => $author_name,
            ];
        }
    }

    return $news_data;
}

function get_term_by_name_taxonomy_parent($term_name, $taxonomy, $parent)
{
    $terms = get_terms(array(
        'taxonomy'   => $taxonomy,
        'name'       => $term_name,
        'hide_empty' => false,
        'parent' => $parent
    ));

    if (! is_wp_error($terms) && ! empty($terms)) {
        foreach ($terms as $term) {
            return $term;
        }
    }

    return false;
}


function get_all_terms_by_parent($parent)
{
    //     $terms = get_terms(array(
    //         'hide_empty' => false,
    //         'taxonomy' => 'news-category',
    //         'parent' => $parent
    //     ));

    global $wpdb;

    $parent = intval($parent);

    $terms = $wpdb->get_results($wpdb->prepare(
        "SELECT t.term_id, t.name, t.slug 
		 FROM {$wpdb->terms} t
		 INNER JOIN {$wpdb->term_taxonomy} tt ON t.term_id = tt.term_id
		 WHERE tt.taxonomy = %s AND tt.parent = %d",
        'news-category',
        $parent
    ));

    if (! is_wp_error($terms) && ! empty($terms)) {
        return $terms;
    }

    return false;
}

////motorcycle

function format_bike_response($bike_ids, $need_variant_info = false)
{
    $bikes = [];
    $posts = get_posts(array(
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => count($bike_ids),
        'post__in' => $bike_ids,
        'orderby' => 'post__in',
    ));

    $bikes = format_bike_response_by_posts($posts, $need_variant_info);

    return $bikes;
}

function format_bike_response_by_posts($posts, $need_variant_info = false)
{
    $bikes = [];

    $post_ids = wp_list_pluck($posts, 'ID');
    $guid_results = get_post_thumbnail_urls($post_ids);
    $required_meta = get_selected_meta_data_for_posts($post_ids, ['listing_state', 'make']);

    foreach ($posts as $post) {
        $bike_id = $post->ID;
        $car_title = $post->post_title;
        $thumbnail_url = $guid_results[$bike_id] ?? '';
        $price_range = get_motor_price_range_of_listing($bike_id);
        $post_name = $post->post_name;
        $listing_state = $required_meta[$bike_id]['listing_state'][0] ?? '';
        $listing_make_id = $required_meta[$bike_id]['make'][0] ?? '';

        $bikes[] = [
            'id' => $bike_id,
            'post_title' => $car_title,
            'thumbnail_url' => $thumbnail_url ?? get_template_directory_uri() . '/images/placeholder.png',
            'price_range' => $price_range,
            'permalink' => get_permalink($bike_id),
            'post_name' => $post_name,
            'listing_state' => $listing_state,
            'listing_make' => get_term($listing_make_id)->name,
            'variants' => [],
        ];

        if ($need_variant_info) {
            $bikes[count($bikes) - 1]['variants'] = get_motor_variant_info($bike_id);
        }
    }

    return $bikes;
}

function get_motor_price_range_of_listing($listing_post_id)
{
    // Fetch all child variants of the listing
    $variants = get_posts(array(
        'post_type' => 'motorcycle-variant',
        'posts_per_page' => -1,
        'post_parent' => $listing_post_id,
        'fields' => 'ids', // Only fetch IDs
    ));

    if (empty($variants)) {
        return 'ยังไม่คอนเฟิร์ม';
    }

    // Use existing helper function to fetch all meta data for variants
    $meta_data = get_post_meta_for_posts($variants);

    // Collect valid prices
    $prices = [];
    foreach ($variants as $variant_id) {
        $price = isset($meta_data[$variant_id]['price'][0])
            ? floatval($meta_data[$variant_id]['price'][0])
            : 0;
        if ($price > 0) {
            $prices[] = $price;
        }
    }

    if (empty($prices)) {
        return 'ยังไม่คอนเฟิร์ม';
    }

    // Calculate price range
    $lowest_price = min($prices);
    $highest_price = max($prices);

    if ($lowest_price === $highest_price) {
        return 'THB ' . format_number_with_commas($highest_price);
    }

    return 'THB ' . format_number_with_commas($lowest_price) . ' - THB ' . format_number_with_commas($highest_price);
}

function get_motor_variant_info($listing_post_id)
{
    $args = array(
        'post_type' => 'motorcycle-variant',
        'posts_per_page' => 5,
        'post_parent' => $listing_post_id,
    );
    $variant_posts = get_posts($args);

    $variants = [];
    foreach ($variant_posts as $variant_post) {
        $variant_id = $variant_post->ID;
        $variant_title = $variant_post->post_title;
        $variant_post_name = $variant_post->post_name;
        $variants[] = [
            'id' => $variant_id,
            'title' => $variant_title,
            'post_name' => $variant_post_name,
        ];
    }

    return $variants;
}
