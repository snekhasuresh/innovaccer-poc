<?php

function fetch_popular_bikes_data_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;

    $recommend_bike_models = get_option('recommended_bike_models');
    $bike_models_data = maybe_unserialize($recommend_bike_models);

    $popular_bike_ids = [];

    if (!empty($bike_models_data) && is_array($bike_models_data)) {
        foreach ($bike_models_data as $category => $category_data) {
            if (isset($category_data['bike_models']) && is_array($category_data['bike_models'])) {
                foreach ($category_data['bike_models'] as $model) {
                    if (is_array($model) && isset($model['id']) && isset($model['type'])) {
                        // Collect the model IDs where type = 1
                        if ($model['type'] == 1 && $model['sort'] >= 0 && $model['sort'] <= 9) {
                            $filtered_models[] = $model;
                        }
                    }
                }
                // Sort the models by 'sort' in ascending order
                usort($filtered_models, function ($a, $b) {
                    return $a['sort'] - $b['sort'];   // Ascending order by 'sort'
                });

                // Now, add the sorted models to the popular_bike_ids array
                foreach ($filtered_models as $model) {
                    $popular_bike_ids[] = $model['id'];
                }
            }
        }
    }

    $popular_bike_response = format_bike_response($popular_bike_ids, $need_variant_info);

    return $popular_bike_response;
}

function fetch_recommended_bikes_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;

    $bike_data = [];
    $recommend_bike_models = get_option('recommended_bike_models');
    $bike_models_data = maybe_unserialize($recommend_bike_models);

    if (!empty($bike_models_data)) {
        foreach ($bike_models_data as $model_category) {
            if (isset($model_category['bike_models']) && is_array($model_category['bike_models'])) {
                $bike_model_ids = wp_list_pluck($model_category['bike_models'], 'id');

                $bike_data[$model_category['category']] = format_car_response($bike_model_ids, $need_variant_info);
            }
        }
    }

    return $bike_data;
}

function fetch_top_bike_models_from_db($args)
{
    $need_variant_info = $args['need_variant_info'] ?? false;

    $car_data = [];
    $petrol_cars_key = 'petrol_cars';
    $suv_cars_key = 'suv_cars';

    $top_bike_models = get_option('top_bike_models', []);
    $suv_data = $top_bike_models[$suv_cars_key] ?? [];
    $petrol_data = $top_bike_models[$petrol_cars_key] ?? [];

    usort($suv_data['bike_models'], function ($a, $b) {
        return $b['position'] <=> $a['position'];
    });
    $top_5_suvs = array_slice($suv_data['bike_models'], 0, 5);
    $suv_car_ids = array_map(function ($car) {
        return $car['id'];
    }, $top_5_suvs);


    usort($petrol_data['bike_models'], function ($a, $b) {
        return $b['position'] <=> $a['position'];
    });
    $top_10_petrol_cars = array_slice($petrol_data['bike_models'], 0, 10);
    $petrol_car_ids = array_map(function ($car) {
        return $car['id'];
    }, $top_10_petrol_cars);

    $car_data['suv_cars'] = format_bike_response($suv_car_ids, $need_variant_info);
    $car_data['petrol_cars'] = format_bike_response($petrol_car_ids, $need_variant_info);

    return $car_data;
}

function fetch_all_top_bike_model_ids_from_db()
{
    $top_bike_model_data = get_option('top_bike_models', []);

    $top_bike_model_ids = array();
    foreach ($top_bike_model_data as $type => $data) {
        $top_bike_model_ids = array_merge($top_bike_model_ids, array_column($data['bike_models'], 'id'));
    }

    return $top_bike_model_ids;
}

function fetch_latest_bikes_data_from_db($args){
	$need_variant_info = $args['need_variant_info'] ?? false;

    $recommend_bike_models = get_option('recommended_bike_models');
    $bike_models_data = maybe_unserialize($recommend_bike_models);

    $latest_bike_ids = [];

    if (!empty($bike_models_data) && is_array($bike_models_data)) {
        foreach ($bike_models_data as $category => $category_data) {
            if ($category === 'popular' && isset($category_data['bike_models']) && is_array($category_data['bike_models'])) {
                foreach ($category_data['bike_models'] as $model) {
                    if (is_array($model) && isset($model['id']) && isset($model['type'])) {
                        // Collect the model IDs where type = 2
                        if ($model['type'] == 2 && $model['sort'] >= 0 && $model['sort'] <= 9) {
                            $filtered_models[] = $model;
                        }
                    }
                }
                // Sort the models by 'sort' in ascending order
                usort($filtered_models, function ($a, $b) {
                    return $a['sort'] - $b['sort'];   // Ascending order by 'sort'
                });

                // Now, add the sorted models to the popular_bike_ids array
                foreach ($filtered_models as $model) {
                    $latest_bike_ids[] = $model['id'];
                }
            }
        }
    }

    $latest_bike_response = format_bike_response($latest_bike_ids, $need_variant_info);

    return $latest_bike_response;
}