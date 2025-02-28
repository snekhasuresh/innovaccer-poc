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
    $variant_post = $global_variant_post_data['variant_post'];
    $variant_post_title = $variant_post->post_title;
    $variant_post_meta = $global_variant_post_data['variant_post_meta'];

    $body_type_taxonomy = get_the_terms($listing_post_id, '_listing_type');
    $body_type_term = get_term($body_type_taxonomy);
    $body_type = $body_type_term->name;

    $length = isset($variant_post_meta['length'][0]) ? $variant_post_meta['length'][0] : '-';
    $width = isset($variant_post_meta['width'][0]) ? $variant_post_meta['width'][0] : '-';
    $height = isset($variant_post_meta['height'][0]) ? $variant_post_meta['height'][0] : '-';
    $dimensions = $length . ' x ' . $width . ' x ' . $height;

    $table_data = [
        'Brand' => ucfirst($make),
        'Body Type' => $body_type,
        'Launched Year' => isset($variant_post_meta['launched_year'][0]) ? $variant_post_meta['launched_year'][0] : '-',
        'Horsepower (ps)' => isset($variant_post_meta['horsepower'][0]) ? $variant_post_meta['horsepower'][0] : '-',
        'Engine' => isset($variant_post_meta['engine'][0]) ? $variant_post_meta['engine'][0] : '-',
        'Length * Width * Height (mm)' => $dimensions,
        'Model' => $variant_post_title,
        'Generation' => isset($variant_post_meta['generation'][0]) ? $variant_post_meta['generation'][0] : '-',
        'Assembly' => isset($variant_post_meta['assembly'][0]) ? $variant_post_meta['assembly'][0] : '-',
        'Torque (Nm)' => isset($variant_post_meta['torque'][0]) ? $variant_post_meta['torque'][0] : '-',
        'Transmission' => isset($variant_post_meta['transmission'][0]) ? $variant_post_meta['transmission'][0] : '-',
        '0-100 kmph (s)' => isset($variant_post_meta['0-100_kmph'][0]) ? $variant_post_meta['0-100_kmph'][0] : '-',
    ];

    ob_start();
?>
    <h2 class="wa-title-text"><?php echo $variant_post_title; ?> Specification</h2>
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
            <a href="<?php echo esc_url($current_page_url . 'specs'); ?>">
                View More <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg"
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
