<?php

function fetch_popular_ev_cars_data_from_db()
{
    $car_categories = get_option('recommended_car_models', []);

    $popular_car_model_ids = [];
    $popular_car_models = [];
	
    foreach ($car_categories as $key => $data) {
        // get only popular category cars
        if ($key == 'Popular') {
            // type is 5 for EV
            // get car models of type 5
            foreach ($data['car_models'] as $car_model) {
				if ($car_model['type'] == 5 && isset($car_model['sort']) && $car_model['sort'] >= 0) {
					$popular_car_models[] = ['id' => $car_model['id'], 'sort' => $car_model['sort']];
				}
			}

			// Sort the popular car models by 'sort' in ascending order
			if (!empty($popular_car_models) && is_array($popular_car_models)) {
				usort($popular_car_models, function ($a, $b) {
					return $a['sort'] - $b['sort']; // Ascending order by 'sort'
				});
			}
        }
    }

//     usort($popular_car_models, function ($a, $b) {
//         return $a['sort'] - $b['sort'];
//     });

    foreach ($popular_car_models as $popular_car_model) {
        $popular_car_model_ids[] = $popular_car_model['id'];
    }

    $posts = get_posts(array(
        'post_type'      => 'listing',
        'posts_per_page' => 10,
        'post__in' => $popular_car_model_ids,
    ));

    $thumbnail_urls = get_post_thumbnail_urls($popular_car_model_ids);

    $popular_cars = [];
    foreach ($posts as $post) {
        $car_id = $post->ID;
        $car_title = $post->post_title;
        $thumbnail_url = isset($thumbnail_urls[$car_id]) ? $thumbnail_urls[$car_id] : '';
        $make_names = wp_list_pluck(wp_get_post_terms($car_id, 'listing_make'), 'name');
        $price_range = get_price_range_of_listing($car_id);

        $popular_cars[] = [
            'id' => $car_id,
            'post_title' => $car_title,
            'thumbnail_url' => $thumbnail_url,
            'price_range' => $price_range,
            'make_names' => $make_names,
        ];
    }

    return $popular_cars;
}
