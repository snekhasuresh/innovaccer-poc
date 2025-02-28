<?php
function fuel_consumption_variant_shortcode()
{

    ob_start();

    $global_listing_data = get_listing_from_query_vars();
    $car_post = $global_listing_data['post'];
    $variant_posts = $global_listing_data['variant_posts'];
    $all_variant_meta = $global_listing_data['variant_meta_data'];

    $table_data = array();

    // Loop through all variant posts and collect their data
    if (!empty($variant_posts)) {
        foreach ($variant_posts as $variant_post) {
            $variant_meta = $all_variant_meta[$variant_post->ID];
            $consumption = isset($variant_meta['manufacturers_claim'][0]) ? $variant_meta['manufacturers_claim'][0] : '';

            // Add each variant's data to the table_data array
            if (!empty($consumption)) {
                $table_data[$variant_post->post_title][] = [
                    'consumption' => $consumption
                ];
            }
        }
    }

    // Display the table
    echo '<h2>' . $car_post->post_title . ' Variant Fuel Consumption</h2>';

    foreach ($table_data as $variant_name => $variant_data) {
        echo '<table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">';
        echo '<tr>';
        echo '<th style="background-color: #f2f2f2; padding: 8px; width: 50%;">' . $variant_name . '</th>';
        echo '<th style="background-color: #f2f2f2; padding: 8px; width: 50%;">Consumption</th>';
        echo '</tr>';
        foreach ($variant_data as $variant) {
            echo '<tr>';
            echo '<td style="padding: 8px;">Manufacturer\'s Claim</td>';
            echo '<td style="padding: 8px;">' . $variant['consumption'] . '</td>';
            echo '</tr>';
        }
        echo '</table>';
    }

    return ob_get_clean();
}

add_shortcode('fuel_consumption_variant', 'fuel_consumption_variant_shortcode');
