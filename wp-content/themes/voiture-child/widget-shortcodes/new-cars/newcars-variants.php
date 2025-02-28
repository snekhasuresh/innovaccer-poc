<?php
// Define the shortcode
function newcar_variant_shortcode()
{
    global $post;

    $post_id = $post->ID;

    // Query for all variants
    $args = array(
        'post_type' => 'variant',
        'meta_query' => array(
            array(
                'key' => 'model',
                'value' => $post_id,
                'compare' => 'LIKE',
            ),
        ),
    );

    $variant_posts = new WP_Query($args);

    // On-sale variants query
    $serialized_value = 's:' . strlen((string)$post_id) . ':"' . $post_id . '";';
    $on_sale_variants_args = array(
        'post_type'  => 'variant',
        'meta_query' => array(
            array(
                'key'     => 'model',
                'value'   => $serialized_value,
                'compare' => 'LIKE',
            ),
            array(
                'key'     => 'on_sale',
                'value'   => 'Yes',
                'compare' => '=',
            ),
        ),
    );

    // Not on-sale variants query
    $not_on_sale_variants_args = array(
        'post_type'  => 'variant',
        'meta_query' => array(
            array(
                'key'     => 'model',
                'value'   => $serialized_value,
                'compare' => 'LIKE',
            ),
            array(
                'key'     => 'on_sale',
                'value'   => 'No',
                'compare' => '=',
            ),
        )
    );

    $variants_on_sale = new WP_Query($on_sale_variants_args);
    $variants_not_on_sale = new WP_Query($not_on_sale_variants_args);

    ob_start(); // Start output buffering

?>
    <div class="row">
        <div class="varient-page-container">
            <div id="listing-detail-description" class="description inner col-md-12">
                <h2 class="variant-car-name wa-title-text"><?php esc_html_e($post->post_title . ' Price List (Variants)', 'voiture'); ?></h2>

                <div class="tabs-container-varient">

                    <div class="custom-tabs-header">
                        <button class="custom-tab-btn custom-tab-active" data-tab="tab-all-variants">
                            <?php esc_html_e('All Variants', 'your-textdomain'); ?>
                        </button>
                        <button class="custom-tab-btn" data-tab="tab-on-sale">
                            <?php esc_html_e('On Sale', 'your-textdomain'); ?>
                        </button>
                        <button class="custom-tab-btn" data-tab="tab-not-on-sale">
                            <?php esc_html_e('Not On Sale Variants', 'your-textdomain'); ?>
                        </button>
                    </div>

                    <div id="tab-all-variants" class="tab-content active">
                        <?php if ($variant_posts->have_posts()) { ?>
                            <div class="car-list-varient">
                                <div class="header">
                                    <span>2022 | 1.5 L | Turbo</span>
                                    <span>Car Price</span>
                                </div>
                                <?php while ($variant_posts->have_posts()) {
                                    $variant_posts->the_post();
                                    $variant_id = get_the_ID();
                                    $meta_data = get_post_meta($variant_id);
                                ?>
                                    <div class="car-item-c">
                                        <div class="car-name"><?php echo esc_html(get_the_title()); ?></div>
                                        <div class="right-content">
                                            <div class="price-section">
                                                <div class="total-price"><?php echo 'RM ' . esc_html($meta_data['retail_price'][0]); ?></div>
                                                <div class="monthly-price"><?php echo 'RM ' . esc_html($meta_data['monthly_payment'][0]) . '/month'; ?></div>
                                            </div>
                                            <div class="calculator-icon">
                                                <!-- SVG icon -->
                                            </div>
                                            <div class="actions">
                                                <a href="#" class="compare-btn"><?php esc_html_e('+ Compare ', 'your-textdomain'); ?></a>
                                                <a href="#" class="trade-btn"><?php esc_html_e(' Trade in for this car ', 'your-textdomain'); ?></a>
                                            </div>
                                        </div>
                                    </div>
                                <?php } ?>
                            </div>
                        <?php } else { ?>
                            <p><?php esc_html_e('No variants found', 'your-textdomain'); ?></p>
                        <?php } ?>
                    </div>

                    <?php if ($variants_on_sale->have_posts()) { ?>
                        <div id="tab-on-sale" class="tab-content">
                            <!-- On Sale variants -->
                        </div>
                    <?php } else { ?>
                        <p><?php esc_html_e('No variants on sale', 'your-textdomain'); ?></p>
                    <?php } ?>

                    <?php if ($variants_not_on_sale->have_posts()) { ?>
                        <div id="tab-not-on-sale" class="tab-content">
                            <!-- Not On Sale variants -->
                        </div>
                    <?php } else { ?>
                        <p><?php esc_html_e('No variants not on sale', 'your-textdomain'); ?></p>
                    <?php } ?>
                </div>
            </div>
        </div>
    </div>

<?php

    wp_reset_postdata();

    return ob_get_clean(); // End buffering and return contents
}

// Register the shortcode [car_variant]
add_shortcode('newcar_variant', 'newcar_variant_shortcode');
