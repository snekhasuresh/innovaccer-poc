<?php

function fetch_popular_cars_data_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;

    $recommend_car_models = get_option('recommended_car_models');
    $car_models_data = maybe_unserialize($recommend_car_models);

    $popular_cars_ids = [];

    if (!empty($car_models_data) && is_array($car_models_data)) {
        foreach ($car_models_data as $category => $category_data) {
            if (isset($category_data['car_models']) && is_array($category_data['car_models'])) {
                foreach ($category_data['car_models'] as $model) {
                    if (is_array($model) && isset($model['id']) && isset($model['type'])) {
						
                        // Collect the model IDs where type = 1
                        if ($model['type'] == 1 && $model['sort'] >= 0 && $model['sort'] <= 9) {
                            $filtered_models[] = $model;
                        }
                    }
                }
                // Sort the models by 'sort' in ascending order
                if (!empty($filtered_models) && is_array($filtered_models)) {
					usort($filtered_models, function ($a, $b) {
						return $a['sort'] - $b['sort'];   // Ascending order by 'sort'
					});
					
					// Now, add the sorted models to the popular_cars_ids array
					foreach ($filtered_models as $model) {
						$popular_cars_ids[] = $model['id'];
					}
				}
                
            }
        }
    }

    $popular_cars_response = format_car_response($popular_cars_ids, $need_variant_info);

    return $popular_cars_response;
}

function fetch_recommended_cars_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;

    $car_data = [];
    $recommend_car_models = get_option('recommended_car_models');
    $car_models_data = maybe_unserialize($recommend_car_models);

    if (!empty($car_models_data)) {
        foreach ($car_models_data as $model_category) {
            if (isset($model_category['car_models']) && is_array($model_category['car_models'])) {
                $car_model_ids = wp_list_pluck($model_category['car_models'], 'id');

                $car_data[$model_category['category']] = format_car_response($car_model_ids, $need_variant_info);
            }
        }
    }

    return $car_data;
}

function fetch_top_car_models_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;

    $car_data = [];
    $petrol_cars_key = 'petrol_cars';
    $suv_cars_key = 'suv_cars';

    $top_car_models = get_option('top_car_models', []);
    $suv_data = $top_car_models[$suv_cars_key] ?? [];
    $petrol_data = $top_car_models[$petrol_cars_key] ?? [];

    usort($suv_data['car_models'], function ($a, $b) {
        return $b['position'] <=> $a['position'];
    });
    $top_5_suvs = array_slice($suv_data['car_models'], 0, 5);
    $suv_car_ids = array_map(function ($car) {
        return $car['id'];
    }, $top_5_suvs);


    usort($petrol_data['car_models'], function ($a, $b) {
        return $b['position'] <=> $a['position'];
    });
    $top_10_petrol_cars = array_slice($petrol_data['car_models'], 0, 10);
    $petrol_car_ids = array_map(function ($car) {
        return $car['id'];
    }, $top_10_petrol_cars);

    $car_data['suv_cars'] = format_car_response($suv_car_ids, $need_variant_info);
    $car_data['petrol_cars'] = format_car_response($petrol_car_ids, $need_variant_info);

    return $car_data;
}

function fetch_all_top_car_model_ids_from_db()
{
    $top_car_model_data = get_option('top_car_models', []);

    $top_car_model_ids = array();
    foreach ($top_car_model_data as $type => $data) {
        $top_car_model_ids = array_merge($top_car_model_ids, array_column($data['car_models'], 'id'));
    }

    return $top_car_model_ids;
}
