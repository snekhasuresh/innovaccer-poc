<?php

function fetch_car_comparison_data_from_db()
{
    $make = get_query_var('make');
    $get_make_data = false;

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'listing_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $current_listing_args  = array(
        'post_type'      => 'listing',
        'orderby'        => 'rand',
    );

    if ($get_make_data) {
        $current_listing_args['meta_query'] = array(
            array(
                'key'     => '_listing_make',
                'value'   => $make_term->term_id,
                'compare' => '='
            )
        );
    }

    $comparison_query = new WP_Query($current_listing_args);

    if ($comparison_query->have_posts()) {
        $post_ids = wp_list_pluck($comparison_query->posts, 'ID');
        // $post_ids = implode(',', $post_ids);
        $thumbnail_urls = get_post_thumbnail_urls($post_ids);
        // thumbnail, price_range, permalink
        foreach ($comparison_query->posts as $post) {
            $price_range = get_price_range_of_listing($post->ID);
            $permalink = get_permalink($post->ID);
            $thumbnail_url = isset($thumbnail_urls[$post->ID]) ? $thumbnail_urls[$post->ID] : '';

            $post->thumbnail_url = $thumbnail_url;
            $post->price_range = $price_range;
            $post->permalink = $permalink;
        }

        return ['posts' => $comparison_query->posts, 'brand_name' =>  $make ? $make_term->name : ''];
    }

    return ['posts' => [], 'brand_name' => $make_term->name];
}

function fetch_grouped_by_type_cars_data_from_db()
{
    global $wpdb;
    $make = get_query_var('make');

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'listing_make');
        if ($make_term) {
            $brand_id = $make_term->term_id;
        }
    } else {
        return [];
    }

    $query = "SELECT t.term_id, t.name, t.slug, tt.taxonomy, tt.count
            FROM {$wpdb->terms} AS t
            INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
            WHERE tt.taxonomy = %s
            ORDER BY t.name ASC
            ";
    $listing_types = $wpdb->get_results($wpdb->prepare($query, 'listing_type'));

    $args = array(
        'post_type'      => 'listing',
        'posts_per_page' => -1,
        'meta_query' => array(
            array(
                'key'     => '_listing_make',
                'value'   => $brand_id,
                'compare' => '='
            ),
        ),
    );

    $all_cars = get_posts($args);

    $all_car_ids = wp_list_pluck($all_cars, 'ID');
    $thumbnail_urls = get_post_thumbnail_urls($all_car_ids);
    $required_meta = get_selected_meta_data_for_posts($all_car_ids, ['listing-state', '_listing_type']);

    foreach ($all_cars as $car) {
        $car_id = $car->ID;
        $car->id = $car_id;
        $car->permalink = get_permalink($car_id);
        $car->thumbnail_url = $thumbnail_urls[$car_id] ?? '';
        $car->price_range = get_price_range_of_listing($car_id);
        $car->listing_state = $required_meta[$car_id]['listing-state'][0] ?? '';
        $car->listing_type = $required_meta[$car_id]['_listing_type'][0] ?? '';
        $car->listing_make = $make_term->name;
    }

    $grouped_cars = [];
    $grouped_cars['All Cars'] = $all_cars;
    foreach ($listing_types as $type) {
        $grouped_cars[$type->name] = array_filter($all_cars, function ($car) use ($type) {
            return $car->listing_type === $type->term_id;
        });
    }

    // remove empty arrays
    $grouped_cars = array_filter($grouped_cars);

    return $grouped_cars;
}

function fetch_grouped_by_type_motors_data_from_db()
{
    global $wpdb;
    $make = get_query_var('make');

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'motorcycle_make');
        if ($make_term) {
            $brand_id = $make_term->term_id;
        }
    } else {
        return [];
    }

    $query = "SELECT t.term_id, t.name, t.slug, tt.taxonomy, tt.count
            FROM {$wpdb->terms} AS t
            INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
            WHERE tt.taxonomy = %s
            ORDER BY t.name ASC
            ";
    $listing_types = $wpdb->get_results($wpdb->prepare($query, 'motorcycle-listing-type'));

    $args = array(
        'post_type'      => 'motorcycle-listing',
        'posts_per_page' => -1,
        'meta_query' => array(
            array(
                'key'     => 'make',
                'value'   => $brand_id,
                'compare' => '=',
				'type'    => 'NUMERIC',
            ),
        ),
    );

    $all_cars = get_posts($args);

    	$all_car_ids = wp_list_pluck($all_cars, 'ID');
		$thumbnail_urls = get_post_thumbnail_urls($all_car_ids);
		$required_meta = get_selected_meta_data_for_posts($all_car_ids, ['listing_state', 'listing_type']);

		foreach ($all_cars as $car) {
			
			$car_id = $car->ID;
			$car->id = $car_id;
			$car->permalink = get_permalink($car_id);
			$car->thumbnail_url = $thumbnail_urls[$car_id] ?? '';
			$car->price_range = get_motor_price_range_of_listing($car_id);
			$car->listing_state = $required_meta[$car_id]['listing_state'][0] ?? '';
			$serialized_type = $required_meta[$car_id]['listing_type'][0] ?? '';
			if (!empty($serialized_type)) {
				// Correct the serialized string if necessary
				$corrected_type = preg_replace_callback(
					'/s:(\d+):"(.*?)";/',
					function ($matches) {
						$actual_length = strlen($matches[2]);
						if ($actual_length != $matches[1]) {
							return 's:' . $actual_length . ':"' . $matches[2] . '";';
						}
						return $matches[0];
					},
					$serialized_type
				);

				// Attempt to unserialize the corrected string
				$car->listing_type = unserialize($corrected_type);
				if ($car->listing_type === false) {
					echo "Error: Failed to unserialize for Car ID: $car_id. Serialized data: $corrected_type\n";
					$car->listing_type = '';
				}
			} else {
				echo "Invalid or empty serialized data for Car ID: $car_id.\n";
				$car->listing_type = '';
			}

			$car->listing_make = $make_term->name;
		}
    $grouped_cars = [];
    $grouped_cars['All'] = $all_cars;

    foreach ($listing_types as $type) {
        $grouped_cars[$type->name] = array_filter($all_cars, function ($car) use ($type) {
//             return $car->listing_type === $type->term_id;
           return in_array($type->term_id, $car->listing_type, true);
        });
    }

    // remove empty arrays
    $grouped_cars = array_filter($grouped_cars);

    return $grouped_cars;
}

function fetch_motor_comparison_data_from_db()
{
    $make = get_query_var('make');
    $get_make_data = false;

    if (!empty($make)) {
        $make_term = get_term_by('slug', $make, 'motorcycle_make');
        if ($make_term) {
            $get_make_data = true;
        }
    }

    $current_listing_args  = array(
        'post_type'      => 'motorcycle-listing',
        'orderby'        => 'rand',
    );

    if ($get_make_data) {
        $current_listing_args['meta_query'] = array(
            array(
                'key'     => 'make',
                'value'   => $make_term->term_id,
                'compare' => '='
            )
        );
    }

    $comparison_query = new WP_Query($current_listing_args);

    if ($comparison_query->have_posts()) {
        $post_ids = wp_list_pluck($comparison_query->posts, 'ID');
        // $post_ids = implode(',', $post_ids);
        $thumbnail_urls = get_post_thumbnail_urls($post_ids);
        // thumbnail, price_range, permalink
        foreach ($comparison_query->posts as $post) {
            $price_range = get_motor_price_range_of_listing($post->ID);
            $permalink = get_permalink($post->ID);
            $thumbnail_url = isset($thumbnail_urls[$post->ID]) ? $thumbnail_urls[$post->ID] : '';

            $post->thumbnail_url = $thumbnail_url;
            $post->price_range = $price_range;
            $post->permalink = $permalink;
        }

        return ['posts' => $comparison_query->posts, 'brand_name' =>  $make ? $make_term->name : ''];
    }

    return ['posts' => [], 'brand_name' => $make_term->name];
}