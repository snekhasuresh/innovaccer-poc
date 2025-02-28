<?php

function add_car_json_ld($make, $model, $specs)
{
    $json_ld_data = [
		"@context" => "https://schema.org",
		"@type" => "Car",
		"brand" => [
			"@type" => "Brand",
			"name" => ucfirst($make),
		],
		"model" => ucfirst($model),
		// Add valid properties for the Car type
		"bodyType" => $specs['Body Type'] ?? '',
		"seatingCapacity" => $specs['Seats'] ?? '',
		"vehicleEngine" => [
			"@type" => "EngineSpecification",
			"engineDisplacement" => $specs['Capacity'] ?? '', // Maps to engineDisplacement
			"enginePower" => $specs['Horsepower'] ?? '' // Maps to enginePower
		],
	];

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
