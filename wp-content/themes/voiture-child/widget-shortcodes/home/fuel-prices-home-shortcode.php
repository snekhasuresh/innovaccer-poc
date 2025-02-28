<?php
function enqueue_fuel_prices_css()
{
    global $post;
    if (isset($post->post_content) && has_shortcode($post->post_content, 'fuel_prices')) {
        wp_enqueue_style(
            'fuel-prices-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/fuel-prices.css',
            array(),
            '1.0',
            'all'
        );
    }
}
add_action('wp_enqueue_scripts', 'enqueue_fuel_prices_css');

add_filter('acf/update_value/name=oil_price', 'update_old_oil_price', 10, 3);

function update_old_oil_price($new_value, $post_id, $field)
{
    // Check if it's an oil post type
    if (get_post_type($post_id) !== 'oil') {
        return $new_value;
    }

    $current_oil_price = get_post_meta($post_id, 'oil_price', true);
    $current_oil_prices = (float) $current_oil_price;
    $new_values = (float) $new_value;

    if ($new_values !== $current_oil_prices && $current_oil_prices !== 0) {
        update_post_meta($post_id, 'old_oil_price', $current_oil_prices);
        update_post_meta($post_id, 'oil_price_change', ($current_oil_prices - $new_values) * 100 / $current_oil_prices);
    }

    return $new_value;
}

function fuel_prices_shortcode()
{
    $fuel_prices_data = get_fuel_price_data();
    $petrol_prices = $fuel_prices_data['petrol_prices'];
    $diesel_prices = $fuel_prices_data['diesel_prices'];
    $formatted_update_time = $fuel_prices_data['latest_update_time'];

    ob_start();
?>
    <div class="fuel-prices-container">
        <h2 class="wa-title-text">Fuel Prices</h2>
        <div class="fuel-prices ">
            <span class="petrol-name">Petrol</span>
            <ul class="petrol-prices">
                <?php foreach ($petrol_prices as $price): ?>
                    <li>
                        <span class="fuel-dot" style="background-color: <?php echo get_color_by_variant($price['variant']); ?>;"></span>
                        <span class="fuel-variant"><?php echo esc_html($price['variant']); ?></span>
                        <span class="fuel-price">RM <?php echo esc_html($price['price']); ?></span>
                    </li>
                <?php endforeach; ?>
            </ul>

            <span class="petrol-name">Diesel</span>
            <ul class="diesel-prices">
                <?php foreach ($diesel_prices as $price): ?>
                    <li>
                        <span class="fuel-dot" style="background-color: <?php echo get_color_by_variant($price['variant']); ?>;"></span>
                        <span class="fuel-variant" style="width: 85px !important;"><?php echo esc_html($price['variant']); ?></span>
                        <span class="fuel-price" style="margin-left: 20px !important;">RM<?php echo esc_html($price['price']); ?></span>
                    </li>
                <?php endforeach; ?>
            </ul>
        </div>

        <p class="last-updated">Last Updated: <?php echo esc_html($formatted_update_time); ?></p>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('fuel_prices', 'fuel_prices_shortcode');

if (!function_exists('get_color_by_variant')) {
    function get_color_by_variant($variant)
    {
        // Define a color mapping for each variant
        $color_map = [
            // Petrol variants
            'RON 95' => '#F9C834',   // Yellow
            'RON 97' => '#179E6C',   // Teal
            'RON 100' => '#CCCCCC',  // Gray
            'VPR' => '#FF2E55',      // Red-Pink

            // Diesel variants
            'EURO 5 B10' => '#222222', // Black
            'EURO 5 B7' => '#3C9FF2'   // Light Blue
        ];

        // Return the color for the specific variant or a default color if not found
        return isset($color_map[$variant]) ? $color_map[$variant] : '#CCCCCC'; // Default gray if no match
    }
}