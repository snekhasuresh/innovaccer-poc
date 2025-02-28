<?php
if (!defined('ABSPATH')) {
    exit;
}

function listing_specs_shortcode($atts)
{
    $atts = shortcode_atts(array(
        'selected_tab' => 'Specs',
        'make' => '',
        'model' => '',
    ), $atts);
    $url = get_site_url() . '/car/' . $atts['make'] . '/' . $atts['model'];

    ob_start();

    echo do_shortcode('[listing_tabs selected_tab="Specs" base_url="' . $url . '" make="' . $atts['make'] . '" model="' . $atts['model'] . '"]');
?>
    <div id="listing-tabs">
        Specs in progress
        <!-- add specs layout here -->
        <!-- take model from $atts and pass to all widgets -->
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('listing_specs', 'listing_specs_shortcode');
