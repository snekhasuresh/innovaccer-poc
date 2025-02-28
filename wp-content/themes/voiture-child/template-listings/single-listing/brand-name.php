<?php
// Fetch car brands (makes) from the 'listing_make' taxonomy
$car_brands = get_terms(array(
    'taxonomy' => 'listing_make',
    'hide_empty' => false, // Set to true to hide empty terms
));

$max_display_count = 12;
$car_brands = array_slice($car_brands, 0, $max_display_count);

?>
<div class="car-brands-title"><?php echo esc_html__('Popular Car Brands in Malaysia', 'voiture'); ?></div>

<div class="car-brands">
    <?php foreach ($car_brands as $brand):
        // Assuming you have a custom field 'listing_make_image' for the logo URL
        $logo_url = get_term_meta($brand->term_id, 'listing_make_image', true);
    ?>
        <div class="car-brand">
            <?php if ($logo_url): ?>
                <img src="<?php echo esc_url($logo_url); ?>" alt="<?php echo esc_attr($brand->name); ?> logo" class="brand-logo">
            <?php endif; ?>
            <span class="brand-name"><?php echo esc_html($brand->name); ?></span>
        </div>
    <?php endforeach; ?>
</div>

<div class="view-more-container">
    <a href="#" class="view-more-button"><?php echo esc_html__('View More', 'voiture'); ?> <span>&#8250;</span></a>
</div>


<!-- Add the following CSS to your theme's stylesheet or within a <style> tag in the template -->
<style>
    /* Add this CSS to your theme's stylesheet */
    .car-brands-title {

        position: relative;
        font-size: 26px;
        line-height: 32px;
        color: #262626;
        font-weight: 700;
        margin-bottom: 20px;
        margin-top: 10px;
    }

    .car-brands {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 0;
        border: 1px solid #ddd;
        border-radius: 4px;
        overflow: hidden;
        width: 100%;
    }

    .car-brand {
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        padding: 10px 0;
        border: 1px solid #ddd;
        /* Apply borders between each item */
    }

    .car-brand:nth-child(-n+6) {
        border-top: none;
        /* Remove the top border for the first row */
    }

    .car-brand:nth-child(6n) {
        border-right: none;
        /* Remove the right-side border for every 6th item */
    }

    .car-brand:nth-child(6n+1) {
        border-left: none;
        /* Remove the left-side border for every 1st item in the row */
    }

    .brand-logo {
        width: 40px;
        /* Smaller logo size */
        height: auto;
        margin-bottom: 5px;
    }

    .brand-name {
        display: block;
        font-size: 14px;
        color: #333;
    }

    .view-more-container {
        display: flex;
        justify-content: center;
        margin-top: 15px;
    }

    .view-more-button {
        font-size: 16px;
        font-weight: bold;
        color: #576B95;
        text-decoration: none;
        border: none;
        background: none;
        cursor: pointer;
        display: flex;
        align-items: center;
    }

    .view-more-button span {
        margin-left: 5px;
        font-size: 18px;
        font-weight: bold;
    }
</style>