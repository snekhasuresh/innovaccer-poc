<?php
function motor_variant_gallery_highlights_shortcode()
{
    global $wpdb;

    $global_variant_post_data = get_variant_from_query_vars();
    if (!$global_variant_post_data) {
        return;
    }

    $current_variant_post = $global_variant_post_data['variant_post'];
    $current_variant_post_title = $current_variant_post->post_title;
    $current_variant_id = $current_variant_post->ID;

    $sql = $wpdb->prepare(
        "SELECT colour, type, image_data FROM car_image WHERE variant_post_id=%d",
        $current_variant_id
    );
    $data = $wpdb->get_results($sql);

    if (empty($data)) {
        return 'No images found for this variant.';
    }

    $images = [
        'interior' => [],
        'exterior' => [],
        'others' => []
    ];

    // Collect images based on type
    foreach ($data as $item) {
        $image_data = json_decode($item->image_data);
        foreach ($image_data as $image) {
            // Limit to 3 images for each type
            if ($item->type === 'Interior' && count($images['interior']) < 3) {
                $index = str_pad(count($images['interior']) + 1, 3, '0', STR_PAD_LEFT);
                $images['interior'][] = [
                    'src' => $image->url,
                    'alt' => $current_variant_post_title . ' Interior ' . $index,
                    'title' => $current_variant_post_title . ' Interior ' . $index,
                    'link' => '/cars/honda/hr-v/car-interior-image-' . $index
                ];
            } elseif ($item->type === 'Exterior' && count($images['exterior']) < 3) {
                $index = str_pad(count($images['exterior']) + 1, 3, '0', STR_PAD_LEFT);
                $images['exterior'][] = [
                    'src' => $image->url,
                    'alt' => $current_variant_post_title . ' Exterior ' . $index,
                    'title' => $current_variant_post_title . ' Exterior ' . $index,
                    'link' => '/cars/honda/hr-v/car-exterior-image-' . $index
                ];
            } elseif ($item->type === 'Others' && count($images['others']) < 3) {
                $index = str_pad(count($images['others']) + 1, 3, '0', STR_PAD_LEFT);
                $images['others'][] = [
                    'src' => $image->url,
                    'alt' => $current_variant_post_title . ' Others ' . $index,
                    'title' => $current_variant_post_title . ' Others ' . $index,
                    'link' => '/cars/honda/hr-v/car-others-image-' . $index
                ];
            }
        }
    }
    // Generate HTML output
    ob_start();
?>
    <h2 class="wa-title-text"><?php esc_html_e($current_variant_post_title . ' Highlight Designs', 'voiture'); ?></h2>


    <div class="gallery-list">
        <?php foreach ($images as $image_type => $image_array) {
            foreach ($image_array as $img) { ?>
                <div class="gallery-list-item">
                    <a href="<?php echo esc_url($img['link']); ?>">
                        <img src="<?php echo esc_url($img['src']); ?>" title="<?php echo esc_attr($img['title']); ?>" alt="<?php echo esc_attr($img['alt']); ?>">
                    </a>
                    <div>
                        <a href="<?php echo esc_url($img['link']); ?>" class="description-link"><?php echo esc_html($img['title']); ?></a>
                    </div>
                </div>
        <?php }
        } ?>

    </div>
    <!-- <div class="view-more-container">
        <a href="/cars/honda/hr-v/gallery" class="view-more-link">View More</a>
    </div> -->
    <style>
        /* Reuse the same styling */
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('motor_variant_highlights_images', 'motor_variant_gallery_highlights_shortcode');
