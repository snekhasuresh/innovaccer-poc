<?php
function motor_gallery_variants_side_shortcode()
{
    global $wpdb;

    $make = get_query_var('make');
    $model = get_query_var('model');
    $listing_name = $make . '-' . $model;

    // get listing post by post name
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1
    ));
    $post_id = $listing_post[0]->ID;

    $parent_variant_data = $wpdb->get_results($wpdb->prepare(
        "SELECT p.ID, p.post_title 
         FROM {$wpdb->posts} p
         INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
         WHERE p.post_parent = %d 
         AND p.post_type = 'motorcycle-variant' 
         AND pm.meta_key = 'state' 
         AND pm.meta_value = '1'",
        $post_id
    ));

    $variantsdata = [];
    foreach ($parent_variant_data as $variant) {
        $variantsdata[$variant->ID] = [
            'name' => $variant->post_title,
            'image_count' => [
                'exterior' => 0,
                'colour' => 0
            ]
        ];
    }

    $ids = array_keys($variantsdata);
    $placeholders = implode(',', array_fill(0, count($ids), '%d'));

    $sql = $wpdb->prepare(
        "SELECT variant_post_id, colour, type, image_data FROM car_image WHERE variant_post_id IN ($placeholders)",
        ...$ids
    );
    $imageResults = $wpdb->get_results($sql);

    foreach ($imageResults as $result) {
        $variant_id = $result->variant_post_id;
        $type = strtolower($result->type);

        // Only process if the variant exists and the type is either interior or exterior
        if (isset($variantsdata[$variant_id]) && in_array($type, ['exterior', 'colour'])) {
            $images = json_decode($result->image_data, true);

            if (is_array($images)) {
                $image_count = count($images);

                $variantsdata[$variant_id]['image_count'][$type] += $image_count;

                $variantsdata[$variant_id]['total_images'] = ($variantsdata[$variant_id]['total_images'] ?? 0) + $image_count;
            }
        }
    }

    $variants = [];
    foreach ($variantsdata as $variant) {
        // Calculate total images (interior + exterior)
        $total_images = ($variant['image_count']['colour'] ?? 0) + ($variant['image_count']['exterior'] ?? 0);

        // Add to new variants array
        $variants[] = [
            'name' => $variant['name'],
            'images' => $total_images
        ];
    }
    // Start output buffering
    ob_start();

    // HTML structure for the widget
?>
    <h2 class="wa-title-text">รูปภาพของรุ่นย่อย <?php echo $listing_post[0]->post_title; ?></h2>

    <div class="honda-hrv-widget">
        <ul class="honda-hrv-list">
            <?php foreach ($variants as $variant) : ?>
                <li>
                    <a href="#" class="variant-name"><?php echo esc_html($variant['name']); ?></a>
                    <a href="#" class="variant-images"><?php echo esc_html($variant['images']); ?> รูปภาพ</a>
                </li>
            <?php endforeach; ?>
        </ul>
    </div>

    <style>
        .honda-hrv-widget {
            /* background-color: #f9f9f9; */
            padding-top: 6px;
            border-radius: 5px;
            width: 100%;
            max-width: 300px;
            border: 1px solid #f5f2f2;
            box-shadow: 0 0 0 1px #f8f7f7 inset, 0 4px 8px 0 rgba(38, 38, 38, .03);
            font-family: 'Roboto';
        }

        .honda-hrv-list {
            list-style: none;
            padding: 0;
            margin: 0;
        }

        .honda-hrv-list li {
            display: flex;
            justify-content: space-between;
            padding: 10px;
            border-bottom: 1px solid #f1eeee;
        }

        .honda-hrv-list li:last-child {
            border-bottom: none;
        }

        .variant-name {
            font-weight: bold;
        }

        .honda-hrv-list a.variant-name {
            text-decoration: none;
            color: #071A40;
            font-size: 14px;
            word-break: break-word;
            flex: 1;
            font-weight: 400;
            line-height: 1.2;
            padding-top: 4px;
        }

        .honda-hrv-list a.variant-images {
            font-size: 14px;
            text-decoration: none;
            white-space: nowrap;
            color: #646566;
            margin-left: 10px;
            align-self: flex-start;
        }



        .variant-images {
            color: #888;
        }
    </style>
<?php

    // End output buffering and return the contents
    return ob_get_clean();
}
add_shortcode('motor_gallery_variants', 'motor_gallery_variants_side_shortcode');
