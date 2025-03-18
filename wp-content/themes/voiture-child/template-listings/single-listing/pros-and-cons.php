<?php
// import css from pros-and-cons.css
function pros_and_cons_css()
{
    wp_enqueue_style('pros-and-cons', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/pros-and-cons.css');
}

function overview_pros_and_cons()
{
    pros_and_cons_css();

    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data || !is_array($global_listing_post_data)) {
        return;
    }

    $post_meta = $global_listing_post_data['post_meta'];

    $pros = isset($post_meta['listing-pros']) ? maybe_unserialize($post_meta['listing-pros'][0]) : '';
    $cons = isset($post_meta['listing-cons']) ? maybe_unserialize($post_meta['listing-cons'][0]) : '';

    if (empty($pros) && empty($cons)) {
        return ''; // Return nothing if no pros or cons exist
    }

    // Get the image URLs for the icons
    $pros_icon_url = wp_get_attachment_image_url(32250, 'props'); // Replace 123 with the actual ID of your "Pros" icon
    $cons_icon_url = wp_get_attachment_image_url(32251, 'cons'); // Replace 124 with the actual ID of your "Cons" icon

    ob_start();
?>
    <div id="listing-detail-description" class="description inner" style="max-width: 1200px; margin: 0 auto;">

        <h2 class="wa-title-text" style="margin-bottom: 20px;"><?php esc_html_e('Ưu điểm & nhược điểm Toyota Raize', 'voiture'); ?></h2>
        <div class="description-inner props-and-cons-con">

            <!-- Pros Section -->
            <div class="pros-section" style="flex: 1; min-width: 45%; padding: 20px; background-color: #F7FAFF; border: 1px solid #E0EFFF; border-radius: 4px; display: flex; flex-direction: column;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                    <img src="<?php echo esc_url($pros_icon_url); ?>" alt="Pros Icon" style="width: 20px; height: 20px;">
                    <h4 class="title" style="margin: 0; color: #007BFF; font-family: Roboto, sans-serif;"><?php esc_html_e('Ưu điểm', 'voiture'); ?></h4>
                </div>
                <div class="pros-content">
                    <?php echo format_wysiwyg_content($pros, '#007BFF'); ?>
                </div>
            </div>

            <!-- Cons Section -->
            <div class="cons-section" style="flex: 1; min-width: 45%; padding: 20px; background-color: #FFF8F5; border: 1px solid #FFE7D9; border-radius: 4px; display: flex; flex-direction: column;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                    <img src="<?php echo esc_url($cons_icon_url); ?>" alt="Cons Icon" style="width: 20px; height: 20px;">
                    <h4 class="title" style="margin: 0; color: #FF5722; font-family: Roboto, sans-serif;"><?php esc_html_e('Nhược điểm', 'voiture'); ?></h4>
                </div>
                <div class="cons-content">
                    <?php echo format_wysiwyg_content($cons, '#FF5722'); ?>
                </div>
            </div>

        </div>
    </div>
<?php
    return ob_get_clean();
}
add_shortcode('overview_pros_and_cons', 'overview_pros_and_cons');


// Function to clean and format the WYSIWYG content
function format_wysiwyg_content($content, $bullet_color)
{
    if (empty($content)) {
        return '<p style="font-style: italic; color: #999;">No data available.</p>';
    }

    // Ensure each <li> gets a custom bullet point
    $content = str_replace('<li>', '<li style="list-style: none; position: relative; padding-left: 20px; margin-bottom: 8px; font-family: Roboto, sans-serif; font-size: 14px; color: #333;">' .
        '<span style="position: absolute; left: 0; top: 8px; width: 6px; height: 6px; background-color: ' . esc_attr($bullet_color) . '; border-radius: 50%;"></span>', $content);

    // Remove default list styles
    $content = preg_replace('/<ol[^>]*>|<ul[^>]*>/', '<ul style="list-style: none; margin: 0; padding: 0;">', $content);
    $content = str_replace('</ol>', '</ul>', $content); // Replace closing </ol> tags if any

    return wp_kses_post($content); // Safeguards the HTML output
}
