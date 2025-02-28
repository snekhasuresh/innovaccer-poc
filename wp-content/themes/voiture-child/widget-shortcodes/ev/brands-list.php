<?php
function enqueue_ev_brands_list_css()
{
    wp_enqueue_style('ev-brands-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/brands-list.css');
}

function display_ev_brands_list_shortcode()
{
    enqueue_ev_brands_list_css();
    // Attempt to retrieve cached data
    $sorted_brands = get_brand_list_ev_data('listing_make_evc');
    $brand_logo = wp_get_attachment_image_url(356072, 'brandlogo');


    $icons = [
        'brandlogo' => $brand_logo,
     
    ];
    // Start the HTML output buffer
    ob_start();
?>

    <div class="brands-list brand-list-ev">
        <?php if (!empty($sorted_brands) && !is_wp_error($sorted_brands)): ?>
            <?php foreach ($sorted_brands as $brand): ?>
                <div class="brand-list-con">
                    <a href="<?php echo esc_url(home_url('/cars-page/' . $brand['slug'])); ?>" class="brand-link">

                        <img src="<?php echo esc_url($brand['image_url']); ?>" alt="<?php echo esc_attr($brand['brand']); ?>" class="brand-logo" />
                    </a>
                    <span class="logo-brand-item"><?php echo esc_html($brand['brand']); ?></span>
                </div>

            <?php endforeach; ?>
        <?php endif; ?>
        <a href="<?php echo home_url('/new-cars/best-ev/'); ?>">
            <div style="display: flex; flex-direction: column; align-items: center;">
                <img src="<?php echo esc_url($icons['brandlogo']); ?>" class="brand-logo" alt="Brand Logo" />
                <span class="logo-brand-item">More</span>
            </div>

        </a>

    </div>
<?php
    // Return the buffer content as a string
    return ob_get_clean();
}
// Register the shortcode

add_shortcode('ev_brands_list', 'display_ev_brands_list_shortcode');
