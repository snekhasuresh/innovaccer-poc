<?php
if (!defined('ABSPATH')) {
    exit;
}

function listing_gallery_shortcode($atts)
{
    $atts = shortcode_atts(array(
        'selectedTab' => 'Gallery',
        'make' => '',
        'model' => '',
    ), $atts);
    $url = get_site_url() . '/car/' . $atts['make'] . '/' . $atts['model'];

    ob_start();

    echo do_shortcode('[listing_tabs selected_tab="Gallery" base_url="' . $url . '" make="' . $atts['make'] . '" model="' . $atts['model'] . '"]');
?>
    <div id="listing-tabs">
        Gallery in progress
        <!-- add gallery layout here -->
        <!-- take model from $atts and pass to all widgets -->
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('listing_gallery', 'listing_gallery_shortcode');
