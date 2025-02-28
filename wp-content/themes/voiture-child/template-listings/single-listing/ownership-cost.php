<?php

function enqueue_overview_ownership_css()
{
    wp_enqueue_style('overview-ownership-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/ownership-cost.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_overview_ownership_css');

function display_ownership_cost()
{
    enqueue_overview_ownership_css();

    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data || !is_array($global_listing_post_data)) {
        return;
    }

    $listing_post = $global_listing_post_data['post'];
    $variant_posts = $global_listing_post_data['variant_posts'];
    if (empty($variant_posts)) {
        return;
    }
    $variant_ids = wp_list_pluck($variant_posts, 'ID');
    $variants_meta_data = $global_listing_post_data['variant_meta_data'];

    // select variant for which either road tax, insurance or fuel cost is available to display
    // if not available for any variant, then display N/A
    $variant_meta = $variants_meta_data[$variant_ids[0]];
    foreach ($variant_ids as $variant_id) {
        $variant_meta = $variants_meta_data[$variant_id];
        if (isset($variant_meta['road_tax'][0]) && $variant_meta['road_tax'][0] !== '' && $variant_meta['road_tax'][0] !== 0) {
            break;
        }
        if (isset($variant_meta['insurance'][0]) && $variant_meta['insurance'][0] !== 0 && $variant_meta['insurance'][0] !== '') {
            break;
        }
        if (isset($variant_meta['fuel_cost'][0]) && $variant_meta['fuel_cost'][0] !== 0 && $variant_meta['fuel_cost'][0] !== '') {
            break;
        }
    }

    $road_tax = isset($variant_meta['road_tax'][0]) ? $variant_meta['road_tax'][0] : 0;
    $insurance = isset($variant_meta['insurance'][0]) ? $variant_meta['insurance'][0] : 0;
    $fuel_cost = isset($variant_meta['fuel_cost'][0]) ? $variant_meta['fuel_cost'][0] : 0;

    ob_start(); // Start output buffering
?>
    <div id="listing-detail-description" class="description inner">
        <div class="ownership-cost-title-con">
            <h2 class="ownership-cost-title wa-title-text"><?php esc_html_e('ค่าใช้จ่าที่คนใช้รถ '.$listing_post->post_title, 'voiture'); ?></h2>

        </div>
        <div class="container-ownership-cost">
            <div class="costs-container">
                <div class="cost-item">
                    <div class="cost-icon road-tax-icon"></div>
                    <span class="cost-label"><?php esc_html_e('ค่าต่อภาษีรถยนต์ประจำปี*', 'your-textdomain'); ?></span>
                    <div class="price-con">
                        <span class="cost-value"><?php echo $road_tax !== 0 ? 'THB ' . $road_tax : 'N/A'; ?></span>
                        <span class="cost-period">/ปี</span>
                    </div>
                </div>

<!--                 <div class="cost-item">
                    <div class="cost-icon insurance-icon"></div>
                    <span class="cost-label"><?php esc_html_e('Insurance Cost*', 'your-textdomain'); ?></span>
                    <div class="price-con">
                        <span class="cost-value"> <?php echo $insurance !== 0 ? 'RM ' . $insurance : 'N/A'; ?></span>
                        <span class="cost-period">/year</span>
                    </div>
                </div> -->

                <div class="cost-item">
                    <div class="cost-icon fuel-cost-icon"></div>
                    <span class="cost-label"><?php esc_html_e('ค่าน้ำมัน*', 'your-textdomain'); ?></span>
                    <div class="price-con">
                        <span class="cost-value"> <?php echo $fuel_cost !== 0 ? 'THB ' . $fuel_cost : 'N/A'; ?></span>
                        <span class="cost-period">/ปี</span>
                    </div>
                </div>

                <div class="cost-item-button" style="justify-content: center;">
                    <button class="calculator-btn">
                        <a href="<?php echo esc_url(home_url('tools/insurance-calculator/')); ?>" style="display: flex; align-items:center;">
                            <svg class="calculator-icon" viewBox="0 0 24 24">
                                <path fill="currentColor" d="M19,3H5C3.9,3,3,3.9,3,5v14c0,1.1,0.9,2,2,2h14c1.1,0,2-0.9,2-2V5C21,3.9,20.1,3,19,3z M19,19H5V5h14V19z" />
                                <path fill="currentColor" d="M7,7h2v2H7V7z M7,11h2v2H7V11z M7,15h2v2H7V15z M11,7h2v2h-2V7z M11,11h2v2h-2V11z M11,15h2v2h-2V15z M15,7h2v2h-2V7z M15,11h2v2h-2V11z M15,15h2v2h-2V15z" />
                            </svg>
                            คำนวณ
                        </a>
                    </button>
                </div>
            </div>

            <p class="footnote">* สำหรับอ้างอิงเท่านั้น คุณสามารถปรับเครื่องคิดเลข ตามสถานการณ์จริงของคุณได้ </p>
        </div>
    </div>
<?php

    return ob_get_clean(); // Return the buffered content
}

// Register the shortcode
add_shortcode('ownership_cost', 'display_ownership_cost');
