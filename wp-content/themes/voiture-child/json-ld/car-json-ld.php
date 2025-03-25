<?php

function add_car_json_ld($make, $model, $specs, $min_price, $max_price, $thumbnail_url, $car_colors, $competitor_ids = [])
{
	// remove make from model
	$model = str_replace($make . ' ', '', $model);
	$name = ucfirst($make) . ' ' . ucfirst($model);

	$json_ld_data = [
		"@context" => "https://schema.org",
		"@type" => "Car",
		"url" => home_url('/xe-oto/' . $make . '/' . $model),
		"sku" => $name,
		"name" => $name,
		"model" => ucfirst($model),
		"bodyType" => $specs['Body Type'] ?? '',
		'color' => $car_colors,
		"image" => [
			"@type" => "ImageObject",
			"contentUrl" => $thumbnail_url,
			"headline" => $name,
		],
		"manufacturer" => [
			"@type" => "Organization",
			"name" => ucfirst($make),
		],
		"brand" => [
			"@type" => "Brand",
			"name" => ucfirst($make),
		],
		// Add valid properties for the Car type
		"seatingCapacity" => $specs['Seats'] ?? '',
		"vehicleEngine" => [
			"@type" => "EngineSpecification",
			"engineDisplacement" => $specs['Capacity'] ?? '', // Maps to engineDisplacement
			"enginePower" => $specs['Horsepower'] ?? '' // Maps to enginePower
		],
		"offers" => [
			"@type" => "AggregateOffer",
			"priceCurrency" => "Triệu",
			"lowPrice" => $min_price,
			"highPrice" => $max_price,
		],
	];
	
	// competitor data
	$similar_car_urls = [];
	foreach ($competitor_ids as $competitor_id) {
		$competitor_post = get_post($competitor_id);

		$competitor_make_id = get_post_meta($competitor_id, '_listing_make', true);
		$competitor_make = get_term($competitor_make_id)->name;
		$competitor_make_slug = get_term($competitor_make_id)->slug;

		$competitor_model = $competitor_post->post_title;
		$competitor_model = str_replace($competitor_make . ' ', '', $competitor_model);
		$competitor_model = sanitize_title($competitor_model);

		$similar_car_urls[] = [
			"@type" => "Car",
			"@id" => home_url('/xe-oto/' . $competitor_make_slug . '/' . $competitor_model)
		];
	}

	$json_ld_data['isSimilarTo'] = $similar_car_urls;

	// Remove empty entries, including nested arrays
	$json_ld_data = array_filter($json_ld_data, function ($value) {
		if (is_array($value)) {
			return !empty(array_filter($value));
		}
		return !empty($value);
	});

	// Remove empty entries in the vehicleConfiguration
	//     $json_ld_data['vehicleConfiguration'] = array_filter($json_ld_data['vehicleConfiguration']);

	$json_ld_script = '<script type="application/ld+json">' . json_encode($json_ld_data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT) . '</script>';

	echo $json_ld_script;
}



function add_motorcycle_json_ld($make, $model, $specs, $min_price, $max_price, $thumbnail_url, $car_colors, $competitor_ids = [])
{
	// remove make from model
	$model = str_replace($make . ' ', '', $model);
	$name = ucfirst($make) . ' ' . ucfirst($model);

	$json_ld_data = [
		"@context" => "https://schema.org",
		"@type" => "Motorcycle",
		"url" => home_url('/xe-may/' . $make . '/' . $model),
		"sku" => $name,
		"name" => $name,
		"model" => ucfirst($model),
		"bodyType" => $specs['Body Type'] ?? '',
		'color' => $car_colors,
		"image" => [
			"@type" => "ImageObject",
			"contentUrl" => $thumbnail_url,
			"headline" => $name,
		],
		"manufacturer" => [
			"@type" => "Organization",
			"name" => ucfirst($make),
		],
		"brand" => [
			"@type" => "Brand",
			"name" => ucfirst($make),
		],
		// Add valid properties for the Car type
		"seatingCapacity" => $specs['Seats'] ?? '',
		"vehicleEngine" => [
			"@type" => "EngineSpecification",
			"engineDisplacement" => $specs['Capacity'] ?? '', // Maps to engineDisplacement
			"enginePower" => $specs['Horsepower'] ?? '' // Maps to enginePower
		],
		"offers" => [
			"@type" => "AggregateOffer",
			"priceCurrency" => "Triệu",
			"lowPrice" => $min_price,
			"highPrice" => $max_price,
		],
	];
	
	// competitor data
	$similar_car_urls = [];
	foreach ($competitor_ids as $competitor_id) {
		$competitor_post = get_post($competitor_id);

		$competitor_make_id = get_post_meta($competitor_id, '_listing_make', true);
		$competitor_make = get_term($competitor_make_id)->name;
		$competitor_make_slug = get_term($competitor_make_id)->slug;

		$competitor_model = $competitor_post->post_title;
		$competitor_model = str_replace($competitor_make . ' ', '', $competitor_model);
		$competitor_model = sanitize_title($competitor_model);

		$similar_car_urls[] = [
			"@type" => "Motorcycle",
			"@id" => home_url('/xe-may/' . $competitor_make_slug . '/' . $competitor_model)
		];
	}

	$json_ld_data['isSimilarTo'] = $similar_car_urls;

	// Remove empty entries, including nested arrays
	$json_ld_data = array_filter($json_ld_data, function ($value) {
		if (is_array($value)) {
			return !empty(array_filter($value));
		}
		return !empty($value);
	});

	// Remove empty entries in the vehicleConfiguration
	//     $json_ld_data['vehicleConfiguration'] = array_filter($json_ld_data['vehicleConfiguration']);

	$json_ld_script = '<script type="application/ld+json">' . json_encode($json_ld_data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT) . '</script>';

	echo $json_ld_script;
}

