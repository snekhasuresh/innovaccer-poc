<?php

// import css from add-car.css
function add_car_css()
{
    wp_enqueue_style('add-car', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/add-car.css');
}

// Define the shortcode function
function car_listing_template_shortcode()
{
    add_car_css();

    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data || !is_array($global_listing_post_data)) {
        return;
    }

    $global_listing_post = $global_listing_post_data['post'];
    $thumbnail = $global_listing_post_data['thumbnail'];
    $post_title = $global_listing_post->post_title;

    ob_start(); // Start output buffering
?>
    <div class="card-container">
        <div class="card">
            <div class="addcar-logo">CARSOME</div>
            <div class="header-card">Get a deal on your trade in within 24 hours!</div>

            <div class="content-card">
                <div class="image-cont">
                    <img src="<?php echo $thumbnail; ?>" class="image-card">
                </div>
                <div class="car-info-side-widget">
                    <div class="car-model-side-widget"><?php echo $post_title ?></div>
                </div>

                <div class="upgrade-text">Upgrade</div>

                <button class="check-price-btn">
                    <a href="<?php echo home_url('/used-car-market-value-guide/'); ?>">Check Your Car Price</a>
                </button>
            </div>

            <div class="footer">
                Not trading in? <a href="https://www.carsome.my/sell-car?utm_source=wapcar&utm_medium=partner&utm_campaign=my-c2b-en-conv-private_seller">Sell your car ›</a>
            </div>
        </div>
        <!-- -------------------------------------------------------------------------------------------------------- -->
        <div class="dropdown" id="dropdown" style="display: none;">
            <div class="alphabet-sidebar" id="alphabetSidebar">
            </div>

            <div class="brand-list" id="brandList">
            </div>
        </div>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('add-new-car', 'car_listing_template_shortcode');
