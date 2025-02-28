<?php

function enqueue_overview_ownership_css()
{
    wp_enqueue_style('overview-ownership-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/overview/car/css/ownership-cost.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_overview_ownership_css');

// Function to display ownership cost
function display_ownership_cost()
{
    enqueue_overview_ownership_css();

    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1,
        'fields' => 'ids'
    ));

    $post_id = $listing_post[0];
    // Modify the WP_Query arguments as per the shortcode requirements
    $args = array(
        'post_type'  => 'motorcycle-variant',
        'fields'         => 'ids',
        'meta_query' => array(
            array(
                'key'     => 'model',
                'value'   => 's:' . strlen((string)$post_id) . ':"' . $post_id . '";',
                'compare' => 'LIKE',
                'limit'   => 1
            )
        )
    );

    // Fetch variant posts
    $variant_posts = new WP_Query($args);

    ob_start(); // Start output buffering

    // Check if variant posts exist or not
    if (empty($variant_posts->posts)) {
        echo '<p>' . esc_html__('No Ownership Cost found.', 'voiture') . '</p>';
    } else {

?>

        <div id="listing-detail-description" class="description inner">
            <?php
            $variant_meta = get_post_meta($variant_posts->posts[0]);
            $road_tax = isset($variant_meta['road_tax'][0]) ? $variant_meta['road_tax'][0] : 0;
            $insurance = isset($variant_meta['insurance'][0]) ? $variant_meta['insurance'][0] : 0;
            $fuel_cost = isset($variant_meta['fuel_cost'][0]) ? $variant_meta['fuel_cost'][0] : 0;
            ?>
            <div class="ownership-cost-title-con">
                <h2 class="ownership-cost-title wa-title-text"><?php esc_html_e(get_the_title($post_id) . ' Ownership Cost', 'voiture'); ?></h2>

            </div>
            <div class="container-ownership-cost">
                <div class="costs-container">
                    <div class="cost-item">
                        <div class="cost-icon road-tax-icon"></div>
                        <span class="cost-label"><?php esc_html_e('Road Tax Cost*', 'your-textdomain'); ?></span>
                        <div class="price-con">
                            <span class="cost-value"><?php echo $road_tax !== 0 ? 'THB ' . $road_tax : 'N/A'; ?></span>
                            <span class="cost-period">/year</span>
                        </div>
                    </div>

                    <div class="cost-item">
                        <div class="cost-icon insurance-icon"></div>
                        <span class="cost-label"><?php esc_html_e('Insurance Cost*', 'your-textdomain'); ?></span>
                        <div class="price-con">
                            <span class="cost-value"> <?php echo $insurance !== 0 ? 'THB ' . $insurance : 'N/A'; ?></span>
                            <span class="cost-period">/year</span>
                        </div>
                    </div>

                    <div class="cost-item">
                        <div class="cost-icon fuel-cost-icon"></div>
                        <span class="cost-label"><?php esc_html_e('Fuel Cost*', 'your-textdomain'); ?></span>
                        <div class="price-con">
                            <span class="cost-value"> <?php echo $fuel_cost !== 0 ? 'THB ' . $fuel_cost : 'N/A'; ?></span>
                            <span class="cost-period">/year</span>
                        </div>
                    </div>

                    <div class="cost-item-button" style="justify-content: center;">
                        <button class="calculator-btn">
                            <svg class="calculator-icon" viewBox="0 0 24 24">
                                <path fill="currentColor" d="M19,3H5C3.9,3,3,3.9,3,5v14c0,1.1,0.9,2,2,2h14c1.1,0,2-0.9,2-2V5C21,3.9,20.1,3,19,3z M19,19H5V5h14V19z" />
                                <path fill="currentColor" d="M7,7h2v2H7V7z M7,11h2v2H7V11z M7,15h2v2H7V15z M11,7h2v2h-2V7z M11,11h2v2h-2V11z M11,15h2v2h-2V15z M15,7h2v2h-2V7z M15,11h2v2h-2V11z M15,15h2v2h-2V15z" />
                            </svg>
                            Calculator
                        </button>
                    </div>
                </div>

                <p class="footnote">* For reference only, you can adjust your real situation with the calculator.</p>
            </div>
        </div>
    <?php
    }
    ?>
<?php

    return ob_get_clean(); // Return the buffered content
}

// Register the shortcode
add_shortcode('ownership_cost', 'display_ownership_cost');
