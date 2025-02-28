<?php

function enqueue_brand_description_css()
{
    wp_enqueue_style('brand-description-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/brand-description.css', array(), '1.0', 'all');
}

function brand_description_shortcode($atts)
{
    enqueue_brand_description_css();

    // Validate brand ID
    if (!isset($atts['brand_id']) || !is_numeric($atts['brand_id'])) {
        return 'Invalid brand ID.';
    }

    $brand_data = get_brand_description_data($atts['brand_id']);

    if (!$brand_data) {
        return 'Brand not found.';
    }

    ob_start();
?>
    <div class="brand-intro-section">
        <h1><?php echo esc_html($brand_data['brand_name']); ?></h1>
        <div class="brand-intro-con">
            <div class="col-md-2">
                <img src="<?php echo esc_url($brand_data['brand_image']); ?>" alt="<?php echo esc_attr($brand_data['brand_name']); ?> logo">
            </div>
            <div class="brand-content-box col-md-10">
                <div class="brand-preview">
                    <?php if (!empty($brand_data['preview_text'])) : ?>
                        <span><?php echo esc_html($brand_data['preview_text']); ?></span>
                    <?php else : ?>
                        <h2 style="font-size: 18px;">รายชื่อรุ่นรถ <?php echo esc_html($brand_data['brand_name']); ?> ใหม่ในไทย</h2>
                    <?php endif; ?>

                    <div id="brand-more-text" class="brand-hidden-text">
                        <?php if (!empty($brand_data['remaining_text'])) : ?>
                            <span><?php echo esc_html($brand_data['remaining_text']); ?></span>
                        <?php endif; ?>

                        <?php if (!empty($brand_data['preview_text'])) : ?>
                            <h2 style="font-size: 18px;">รายชื่อรุ่นรถ <?php echo esc_html($brand_data['brand_name']); ?> ใหม่ในไทย</h2>
                        <?php endif; ?>

                        <div class="car-container">
                            <?php foreach ($brand_data['grouped_models'] as $type => $models): ?>
                                <div class="car-row">
                                    <div class="dis-label"><?php echo esc_html($brand_data['brand_name'] . ' ' . $type); ?></div>
                                    <div class="dis-value" style="text-align: start;">
                                        <?php echo esc_html(implode(', ', $models)); ?>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>

                        <h2 style="font-size: 18px;">ตารางราคา <?php echo esc_html(ucfirst($brand_data['brand_name'])); ?></h2>
                        <div class="car-container">
                            <div class="car-row dis-header">
                                <div class="dis-label">ตารางรุ่นรถของ <?php echo esc_html($brand_data['brand_name']); ?></div>
                                <div class="dis-value">ราคา</div>
                            </div>

                            <?php foreach ($brand_data['models_with_prices'] as $modelprice): ?>
                                <div class="car-row">
                                    <div class="dis-label">ราคา <?php echo esc_html($modelprice['name']); ?></div>
                                    <div class="dis-value" style="text-align: start;">
                                        <?php echo esc_html($modelprice['price']); ?>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    </div>
                </div>
                <button id="desc-read-more-btn" class="desc-read-more-btn">อ่านเพิ่มเติม</button>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const moreTextToggle = document.getElementById("desc-read-more-btn");
            const moreText = document.getElementById("brand-more-text");
            const brandPreview = document.querySelector(".brand-preview");

            moreTextToggle.addEventListener("click", function() {
                const isHidden = moreText.classList.contains("brand-hidden-text");

                moreText.classList.toggle("brand-hidden-text");
                moreTextToggle.innerHTML = isHidden ? "ซ่อน" : "อ่านเพิ่มเติม";

                brandPreview.style.maxHeight = isHidden ? "none" : "150px";
                brandPreview.style.overflowY = isHidden ? "auto" : "hidden";
            });
        });
    </script>
<?php

    return ob_get_clean();
}

add_shortcode('brand_description', 'brand_description_shortcode');
