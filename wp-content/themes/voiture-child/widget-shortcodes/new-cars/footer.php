<?php
function conditional_cars__footer_template_shortcode()
{
    $current_url = $_SERVER['REQUEST_URI'];

    $template_cars_shortcode = '[elementor-template id="728543"]';
    $template_brand_cars_shortcode = '[elementor-template id="732244"]';

    // Check the URL and render the appropriate template
    if (strpos($current_url, '/cars') !== false) {
        return do_shortcode($template_cars_shortcode);
    } else {
        return do_shortcode($template_brand_cars_shortcode);
    }
}

add_shortcode('cars_footer', 'conditional_cars__footer_template_shortcode');
