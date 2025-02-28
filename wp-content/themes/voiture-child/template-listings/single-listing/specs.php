<?php
if (!defined('ABSPATH')) {
    exit;
}
global $post;
?>
<div>
    <?php
    if (!defined('ABSPATH')) {
        exit;
    }
    global $post;
    global $wpdb;
    $overview_tabs = ['Overview', 'News', 'Specs', 'Gallery'];
    $car_post_meta = get_post_meta($post->ID);
    // $segment = $car_post_meta['listing-segment'][0];
    // $body_type_term_id = $car_post_meta['_listing_type'][0];
    // $body_type = get_term_by('id', $body_type_term_id, 'listing_type')->name;

    // get variants (post) of current listing
    $variant_posts = get_posts(array(
        'post_type'      => 'variant',
        'posts_per_page' => -1,
        'meta_query'     => array(
            array(
                'key'     => 'model',
                'value'   => 's:' . strlen((string)$post->ID) . ':"' . $post->ID . '";',
                'compare' => 'LIKE',
            ),
        ),
    ));

    // calculate min and max of horsepower, capacity, seats
    $min_horsepower = $max_horsepower = $min_capacity = $max_capacity = $min_seats = $max_seats = 0;
    $transmissions = array();
    $horsepower = 0;
    $capacity = 0;
    $seats = 0;

    foreach ($variant_posts as $variant) {
        $variant_meta = get_post_meta($variant->ID);
        if (isset($variant_meta['horsepower'][0])) {
            $horsepower = $variant_meta['horsepower'][0];
            $min_horsepower = $horsepower < $min_horsepower ? $horsepower : $min_horsepower;
            $max_horsepower = $horsepower > $max_horsepower ? $horsepower : $max_horsepower;
        }

        if (isset($variant_meta['capacity'][0])) {
            $capacity = $variant_meta['capacity'][0];
            $min_capacity = $capacity < $min_capacity ? $capacity : $min_capacity;
            $max_capacity = $capacity > $max_capacity ? $capacity : $max_capacity;
        }

        if (isset($variant_meta['seats'][0])) {
            $seats = $variant_meta['seats'][0];
            $min_seats = $seats < $min_seats ? $seats : $min_seats;
            $max_seats = $seats > $max_seats ? $seats : $max_seats;
        }

        if (isset($variant_meta['transmission'][0])) {
            $transmissions[] = $variant_meta['transmission'][0];
        }
    }

    $transmissions = array_unique($transmissions);
    $horsepower = $capacity = $seats = '';

    if ($min_horsepower == $max_horsepower || $min_horsepower == 0) {
        $horsepower = $max_horsepower;
    }
    if ($min_capacity == $max_capacity || $min_capacity == 0) {
        $capacity = $max_capacity;
    }
    if ($min_seats == $max_seats || $min_seats == 0) {
        $seats = $max_seats;
    }

    $specs = array(
        'Segment' => $segment,
        'Transmission' => implode(', ', $transmissions),
        'Body Type' => $body_type,
        'Capacity' => $capacity . 'L',
        'Horsepower' => $horsepower . 'hp',
        'Seats' => $seats,
    )
    ?>
    <div id="listing-detail-detail" class="listing-detail-detail">
        <h3 class="widget-title">Specs</h3>
        <div class="specs-container">
            <?php foreach ($specs as $key => $value) : ?>
                <div class="spec-field">
                    <span class="field-label"><?php echo $key; ?></span>
                    <span class="field-value"><?php echo $value; ?></span>
                </div>
            <?php endforeach; ?>
        </div>

        <div class="buttons-container">
            <button class="view-specs-button">View Specs</button>
            <button class="trade-in-button1">Trade In For This Car</button>
        </div>

        <?php do_action('wp-cardealer-single-listing-description', $post); ?>

        <!-- <style>
            .specs-container {
                display: flex;
                flex-wrap: wrap;
                gap: 20px;
            }

            .spec-field {
                width: calc(33.33% - 20px);
                border: 1px solid #ccc;
                border-radius: 8px;
                padding: 10px;
                display: flex;
                flex-direction: column;
                align-items: left;
            }

            .buttons-container {
                display: flex;
                flex-direction: column;
                margin-top: 20px;
            }


            .trade-in-button1 {
                background-color: #000080;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 8px;
                cursor: pointer;
            }

            .field-label {
                font-size: 10px;
            }

            .field-value {
                font-size: 14px;
                font-weight: bold;
                color: black;
            }
        </style> -->
    </div>