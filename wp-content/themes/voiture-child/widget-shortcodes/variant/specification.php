<?php

function car_variant_specification_shortcode($atts)
{
    // read atts
    $atts = shortcode_atts(array(
        'selected_tab' => 'Overview',
    ), $atts);

    $make = get_query_var('make');

    $global_variant_post_data = get_variant_from_query_vars();

    if (!$global_variant_post_data) {
        return;
    }

    $listing_post_id = $global_variant_post_data['listing_post']->ID;
    $listing_post_title = $global_variant_post_data['listing_post']->post_title;
    $variant_post = $global_variant_post_data['variant_post'];
    $variant_post_title = $variant_post->post_title;
    $variant_post_meta = $global_variant_post_data['variant_post_meta'];

	$listing_post_meta = get_post_meta($listing_post_id);
	$segment = $listing_post_meta['listing-segment'][0];
	
    $body_type_taxonomy = get_the_terms($listing_post_id, '_listing_type');
	if (!is_wp_error($body_type_taxonomy) && !empty($body_type_taxonomy)) {
    // Get the first term (if multiple terms are returned).
    $body_type_term = reset($body_type_taxonomy); 

    // Access the term name.
    $body_type = $body_type_term->name;
    print_r($body_type); // Output the body type.
} else {
    // Handle the error or no terms found.
    if (is_wp_error($body_type_taxonomy)) {
        echo 'Error: ' . $body_type_taxonomy->get_error_message();
    } else {
        echo 'No terms found for listing_type taxonomy.';
    }
}

    $length = isset($variant_post_meta['length'][0]) ? $variant_post_meta['length'][0] : '-';
    $width = isset($variant_post_meta['width'][0]) ? $variant_post_meta['width'][0] : '-';
    $height = isset($variant_post_meta['height'][0]) ? $variant_post_meta['height'][0] : '-';
    $dimensions = $length . ' x ' . $width . ' x ' . $height;

    $table_data = [
        'Brand' => ucfirst($make),
		'Model' => $listing_post_title,
		'Segement' => $segment,
        'Tipe bodi' => $body_type,
		'Hộp số' => isset($variant_post_meta['transmission'][0]) ? $variant_post_meta['transmission'][0] : '-',
        'Dung tích' => isset($variant_post_meta['capacity'][0]) ? $variant_post_meta['capacity'][0] : '-',
        'Công suất cực đại' => isset($variant_post_meta['engine_power'][0]) ? $variant_post_meta['engine_power'][0] : '-',
        'Chỗ ngồi' => isset($variant_post_meta['seats'][0]) ? $variant_post_meta['seats'][0] : '-',
    ];

    ob_start();
?>
    <h2 class="wa-title-text">Thông Số kỹ Thuật <?php echo $variant_post_title; ?></h2>
    <table class="variant-specification-table">
        <?php
        foreach ($table_data as $key => $value) : ?>
            <tr>
                <th><?php echo $key; ?></th>
                <td><?php echo $value; ?></td>
            </tr>
        <?php endforeach; ?>
    </table>
    <div class="btn-more-container">
        <button class="btn-more">
            <?php $current_page_url = $_SERVER['REQUEST_URI']; ?>
            <a href="<?php echo esc_url($current_page_url . 'tieu-hao-nhien-lieu'); ?>">
                Xem thêm <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg"
                    viewBox="0 0 320 512">
                    <path d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5 12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                </svg>
            </a>
        </button>
    </div>

    <style>
        .variant-specification-table {
            width: 100%;
            border-collapse: collapse;
            font-family: Arial, sans-serif;
        }

        .variant-specification-table th,
        .variant-specification-table td {
            padding: 10px;
            border-bottom: 1px solid #ddd;
            text-align: left;
        }

        .variant-specification-table th {
            background-color: #f2f2f2;
            font-weight: bold;
        }

        .variant-specification-table td {
            color: #666;
        }
    </style>

<?php

    $output = ob_get_clean();

    return $output;
}

add_shortcode('car_variant_specification', 'car_variant_specification_shortcode');
