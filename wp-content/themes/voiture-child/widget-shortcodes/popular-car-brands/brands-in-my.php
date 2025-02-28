<?php
function enqueue_cars_brands_css()
{
    // Register and enqueue the CSS file
    wp_enqueue_style('cars-brands-style', get_stylesheet_directory_uri() . '/widget-shortcodes/popular-car-brands/brands-in-my.css', array(), '1.0', 'all');
}
add_action('wp_enqueue_scripts', 'enqueue_cars_brands_css');

// Shortcode function to display popular car brands
function display_popular_car_brands()
{
    ob_start(); // Start output buffering
    $cache_key = 'brands_list';
    $sorted_brands = get_transient($cache_key);
    if (false === $sorted_brands) {
        $car_brands = get_terms(array(
            'taxonomy' => 'listing_make',
            'hide_empty' => false, // Set to true to hide empty terms
        ));

        $sorted_brands = [];
        if (!is_wp_error($car_brands) && !empty($car_brands)) {
            $brands_with_sort = [];

            // Filter brands based on their 'state' and sort by 'sort_order'
            foreach ($car_brands as $brand) {
                $state = get_term_meta($brand->term_id, 'state', true);
                if ($state == 1) {
                    $sort_order = get_term_meta($brand->term_id, 'sort', true);
                    $sort_order = ($sort_order !== '') ? intval($sort_order) : 9999;
                    $brands_with_sort[] = [
                        'brand' => $brand,
                        'sort_order' => $sort_order,
                    ];
                }
            }

            // Sort the brands by their 'sort_order'
            usort($brands_with_sort, function ($a, $b) {
                return $a['sort_order'] <=> $b['sort_order'];
            });

            // Remove duplicate sort orders and limit to 12 brands
            $seen_sort_orders = [];

            foreach ($brands_with_sort as $item) {
                if (!in_array($item['sort_order'], $seen_sort_orders)) {
                    $seen_sort_orders[] = $item['sort_order'];
                    $sorted_brands[] = $item['brand'];
                }
            }

            // Limit to 12 brands max
            $sorted_brands = array_slice($sorted_brands, 0, 12);
            set_transient($cache_key, $sorted_brands, HOUR_IN_SECONDS);
        }
    }

?>
    <h2 class="car-brands-title wa-title-text"><?php echo esc_html__('Famous Car Brands in Indonesia', 'voiture'); ?></h2>

    <div class="car-brands">
        <?php foreach ($sorted_brands as $brand):
            // Assuming you have a custom field 'listing_make_image' for the logo URL
            $logo_url = get_term_meta($brand->term_id, 'listing_make_image', true);
        ?>
            <div class="car-brand" style="display: flex; flex-direction: column; align-items: center;">
                <a href="<?php echo esc_url(home_url('/cars/' . $brand->slug)); ?>" class="brand-link">
                    <?php if ($logo_url): ?>
                        <img src="<?php echo esc_url($logo_url); ?>" alt="<?php echo esc_attr($brand->name); ?> logo" class="brand-logo">
                    <?php endif; ?>
                </a>
                <span class="brand-name"><?php echo esc_html($brand->name); ?></span>
            </div>

        <?php endforeach; ?>
    </div>

    <div class="view-more-container">
        <a href="<?php echo esc_url(home_url('/cars')); ?>" class="view-more-button"><?php echo esc_html__('See More', 'voiture'); ?> <span>&#8250;</span></a>
    </div>
<?php

    // Get the buffered content and end buffering
    return ob_get_clean();
}

// Register the shortcode
add_shortcode('popular_car_brands', 'display_popular_car_brands');
?>