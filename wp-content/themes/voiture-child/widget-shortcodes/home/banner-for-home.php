<?php
/**
 * Enqueue CSS and JS for the banner carousel only if the 'home_banner_slide' shortcode is present in the post content.
 */
function enqueue_banner_css()
{
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'home_banner_slide')) {
        wp_enqueue_style('banner-style', get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/banner-home.css', array(), '1.0.0');
        wp_enqueue_script('banner-script', get_stylesheet_directory_uri() . '/widget-shortcodes/home/js/banner.js', array(), null, true);
//     }
}

add_action('wp_enqueue_scripts', 'enqueue_banner_css');

/**
 * Shortcode for rendering the sale carousel banner.
 *
 * This function retrieves carousel data, processes it, and outputs the HTML structure
 * for the carousel. If no data is available, nothing will be rendered.
 *
 * @return string Rendered HTML for the carousel.
 */
function merdecar_sale_carousel_shortcode() {
	enqueue_banner_css();
    $carousel_data = get_carousel_data();

    ob_start();
    if (!empty($carousel_data)) : ?>
        <div class="merdecar-carousel">
            <div class="carousel-container">
                <?php foreach ($carousel_data as $item) : ?>
                    <div class="carousel-slide">
                        <a href="<?php echo esc_url($item['url']); ?>" class="full-link">
                            <img src="<?php echo esc_url($item['image_guid']); ?>" alt="<?php echo esc_attr($item['title']); ?>">
                        </a>
                    </div>
                <?php endforeach; ?>
            </div>
            <button class="carousel-prev">❮</button>
            <button class="carousel-next">❯</button>
        </div>
    <?php endif;

    return ob_get_clean();
}

add_shortcode('home_banner_slide', 'merdecar_sale_carousel_shortcode');
