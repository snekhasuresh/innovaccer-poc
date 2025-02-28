<?php

function fuel_consumption_info_shortcode()
{
    ob_start();
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    $global_listing_data = get_listing_from_query_vars();
    $car_post = $global_listing_data['post'];
    $variant_posts = $global_listing_data['variant_posts'];
    $all_variant_meta = $global_listing_data['variant_meta_data'];

    $table_data = array();
    $lowest_manufacturer_claim = 999999999;
    $highest_manufacturer_claim = 0;
    // Loop through all variant posts and collect their data
    if (!empty($variant_posts)) {
        foreach ($variant_posts as $v => $variant_post) {
            $variant_meta = $all_variant_meta[$variant_post->ID];
            $consumption = isset($variant_meta['manufacturers_claim'][0]) ? $variant_meta['manufacturers_claim'][0] : '';

            // split by " " and take the first word and convert to number
            if (!empty($consumption)) {
                $consumption = floatval(explode(" ", $consumption)[0]);

                if ($consumption < $lowest_manufacturer_claim) {
                    $lowest_manufacturer_claim = $consumption;
                }

                if ($consumption > $highest_manufacturer_claim) {
                    $highest_manufacturer_claim = $consumption;
                }
            }


            // Add each variant's data to the table_data array
            if (!empty($consumption)) {
                $table_data[$variant_post->post_title][] = [
                    'consumption' => $consumption
                ];
            }
        }

        if ($lowest_manufacturer_claim == 999999999) {
            $lowest_manufacturer_claim = '--';
        }

        if ($highest_manufacturer_claim == 0) {
            $highest_manufacturer_claim = '--';
        }
    }

    echo '<h1>' . $car_post->post_title . ' Fuel Consumption</h1>';

    echo '<div class="fuel-consumption-container">';
    echo "<p>The fuel consumption of the " . $car_post->post_title . " is " . $lowest_manufacturer_claim . " (the most fuel-efficient), and the highest fuel consumption is " . $highest_manufacturer_claim . ".</p>";
    echo "<p>Fuel consumption is most intuitively measured as the fuel required to travel a unit distance, which is known as L/100 kilometers.</p>";

    echo "<div id='fuel-consumption-content'>";
    echo "<p>Here are the fuel consumption rates for " . $car_post->post_title . " cars, according to " . ucfirst($make) . " official:</p>";

    foreach ($table_data as $variant => $data) {
        echo '<p>The fuel consumption of ' . $variant . ' is ' . $data[0]['consumption'] . '.</p>';
    }

    echo "<p>Fuel economy, as the inverse of fuel consumption, is another common indicator, which is calculated as the distance traveled per unit of fuel, such as kilometers/liter or miles/gallon.</p>";
    echo "<p>The fuel consumption of a car mainly depends on its power technology and size. These variables include:</p>";
    echo "<ul>
            <li>1. Road, traffic, and weather conditions</li>
            <li>2. Driving style</li>
            <li>3. Vehicle speed, load, and condition</li>
          </ul>";
    echo "<p>The following formula is used to calculate fuel consumption in liters/100km, which is the most commonly used measure of fuel consumption:</p>";
    echo "<p>(Liters used × 100) ÷ km traveled = litres per 100 kilometers.</p>";
    echo "<p>In this way, you can easily get the monthly fuel cost of " . $car_post->post_title . " by using our fuel cost calculator.</p>";
    echo "</div>";

    echo '<span id="toggle-button" onclick="toggleContent()">View More</span>';
    echo '</div>';

?>
    <script>
        function toggleContent() {
            var content = document.getElementById("fuel-consumption-content");
            var button = document.getElementById("toggle-button");

            if (content.style.display === "none" || content.style.display === "") {
                content.style.display = "block";
                button.innerText = "Hide";
            } else {
                content.style.display = "none";
                button.innerText = "View More";
            }
        }
    </script>
    <style>
        .fuel-consumption-container {
            background-color: #f0f0f0;
            /* Light grey background */
            padding: 20px;
            border-radius: 8px;
            position: relative;
        }

        .fuel-consumption-table {
            font-family: 'Roboto' !important;
            color: #262626 !important;
        }

        #fuel-consumption-content {
            display: none;
            margin-bottom: 20px;
        }

        #toggle-button {
            font-weight: bold;
            color: #5D3A3A;
            /* Brinjal color */
            cursor: pointer;
            position: absolute;
            bottom: 10px;
            right: 10px;
        }

        #toggle-button:hover {
            text-decoration: underline;
        }
    </style>
<?php

    return ob_get_clean();
}

add_shortcode('fuel_consumption_info', 'fuel_consumption_info_shortcode');
