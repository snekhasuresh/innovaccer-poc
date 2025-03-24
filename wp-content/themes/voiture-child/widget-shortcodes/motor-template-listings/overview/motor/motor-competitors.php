<?php
function enqueue_motor_overview_competitors_css()
{
        wp_enqueue_style('overview-competitors-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/competitors.css', array(), '1.0', 'all');
}
function motor_competitors_shortcode()
{
    enqueue_motor_overview_competitors_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    $min_price = $global_listing_post_data['min_price'];
    $max_price = $global_listing_post_data['max_price'];
    $listing_post = $global_listing_post_data['post'];
    $post_id = $listing_post->ID;
    $post_title = $listing_post->post_title;
    $listing_meta = $global_listing_post_data['post_meta'];
    $listing_type = $listing_meta['_listing_type'];


    if ($min_price === null || $max_price === null) {
        return;
    }

    $competitor_post_ids = get_distinct_motor_variants_by_price($min_price, $max_price, $post_id);

    if (empty($competitor_post_ids)) {
        return;
    }

    $car_data = format_bike_response($competitor_post_ids);

    $competitors = [];

    foreach ($car_data as $competitor) {
        $model_name = $competitor['post_title'];
        $listing_state = $competitor['listing_state'];
        $img_url = $competitor['thumbnail_url'];
        $permalink = $competitor['permalink'];

        // Check if the listing state is 'On Sale'
        if ($listing_state === 'On Sale') {
            $price_display = $competitor['price_range'];
        } else {
            $price_display = 'ยังไม่คอนเฟิร์ม';
        }

        // Add the competitor data to the array
        $competitors[] = array(
            'name' => $model_name,
            'price' => $price_display,
            'img' => $img_url,
            'car_url' => $permalink
        );
    }
    wp_reset_postdata();

    ob_start();
?>
    <div class="competitors">
        <span class="title-com"><?php esc_html_e('Đối Thủ Của ' . $post_title, 'voiture'); ?> </span>
        <div id="competitorsList">
            <?php foreach ($competitors as $competitor) : ?>
                <a href="<?php echo $competitor['car_url']; ?>" class="competitor">
                    <img src="<?php echo $competitor['img']; ?>" alt="<?php echo esc_attr($competitor['name']); ?>">
                    <div class="details">
                        <span class="car-name-comp"><?php echo esc_html($competitor['name']); ?></span>
                        <span class="car-price-comp"><?php echo esc_html($competitor['price']); ?></span>
                    </div>
                </a>
            <?php endforeach; ?>
        </div>
    </div>

<?php
    return ob_get_clean();
}

add_shortcode('motor_competitors', 'motor_competitors_shortcode');

function get_distinct_motor_variants_by_price($min_price, $max_price, $post_id)
{
    global $wpdb;
    $query = $wpdb->prepare(
        "SELECT DISTINCT p.post_parent, pm.meta_value 
        FROM {$wpdb->posts} AS p
        INNER JOIN {$wpdb->postmeta} AS pm ON p.ID = pm.post_id
        WHERE p.post_type = %s 
        AND pm.meta_key = %s 
        AND pm.meta_value BETWEEN %d AND %d 
        AND p.post_parent != %d limit 5",
        'motorcycle-variant',
        'price',
        $min_price,
        $max_price,
        $post_id
    );

    $results = $wpdb->get_results($query);

    $post_ids = wp_list_pluck($results, 'post_parent');

    return $post_ids;
}
