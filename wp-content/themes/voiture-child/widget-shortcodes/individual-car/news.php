<?php
if (!defined('ABSPATH')) {
    exit;
}

function listing_news_shortcode($atts)
{
    $atts = shortcode_atts(array(
        'selectedTab' => 'News',
        'make' => '',
        'model' => '',
    ), $atts);
    $url = get_site_url() . '/car/' . $atts['make'] . '/' . $atts['model'];

    ob_start();

    echo do_shortcode('[listing_tabs selected_tab="News" base_url="' . $url . '" make="' . $atts['make'] . '" model="' . $atts['model'] . '"]');
?>
    <div id="listing-tabs">
        News in progress
        <!-- add news layout here -->
        <!-- take model from $atts and pass to all widgets -->
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('listing_news', 'listing_news_shortcode');
