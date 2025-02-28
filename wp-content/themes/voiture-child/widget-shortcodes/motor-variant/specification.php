<?php

function motor_variant_specification_shortcode($atts)
{
    // read atts
    $atts = shortcode_atts(array(
        'selected_tab' => 'Overview',
    ), $atts);

    global $wpdb;

    $make  = get_query_var('make');
    $model = get_query_var('model');

    $global_variant_post_data = get_motor_variant_from_query_vars();

    if (!$global_variant_post_data) {
        return;
    }

    $listing_post_id = $global_variant_post_data['listing_post']->ID;
    $model_title = $global_variant_post_data['listing_post']->post_title;
    $variant_post = $global_variant_post_data['variant_post'];
    $variant_post_title = $variant_post->post_title;
    $variant_post_meta = $global_variant_post_data['variant_post_meta'];

    $body_type_taxonomy = get_the_terms($listing_post_id, 'listing_type');
    $body_type_term = get_term($body_type_taxonomy);
    $body_type = $body_type_term->name;

    $capacity = isset($variant_post_meta['capacity'][0]) ? $variant_post_meta['capacity'][0] : '-';
    $maximum_power = isset($variant_post_meta['maximum_power'][0]) ? $variant_post_meta['maximum_power'][0] : '-';
    $engine_opening_option = isset($variant_post_meta['engine_opening_option'][0]) ? $variant_post_meta['engine_opening_option'][0] : '-';

    $price = isset($variant_post_meta['price'][0]) ? 'THB ' . format_number_with_commas($variant_post_meta['price'][0]) : 'ยังไม่คอนเฟิร์ม';
    $monthly_payment = isset($variant_post_meta['monthly_payment'][0]) ? 'THB ' . format_number_with_commas($variant_post_meta['monthly_payment'][0]) . '/เดือน' : 'ยังไม่คอนเฟิร์ม';

	$images_sql = $wpdb->prepare(
                    "SELECT type, image_data FROM car_image WHERE variant_post_id = %d",
                    $variant_post->ID
                );

    $imageResults = $wpdb->get_results($images_sql);
	// Initialize an empty array for images
	$firstThreeImages = [];

	// Check if we have results
	if (!empty($imageResults)) {
		// Decode the image_data JSON for the first result (assuming only one result is returned)
		$imageData = json_decode($imageResults[0]->image_data, true);

		if (!empty($imageData)) {
			// Extract the first three images
			$firstThreeImages = array_slice($imageData, 0, 3);
		}
	}
    $base_url = home_url('/motorcycles/') . $make . '/' . $model . '/';

	//get model variants 
    $parent_variant_ids = $wpdb->get_results($wpdb->prepare(
        "SELECT ID, post_title FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'motorcycle-variant'",
        $listing_post_id
    ));

    $filtered_variants = [];
    foreach ($parent_variant_ids as $variant) {
        $state = get_post_meta($variant->ID, 'state', true); // Replace 'state' with your actual meta key if different
        if ($state == 1) {
            $filtered_variants[] = $variant; // Keep only the variants where state is 1
        }
    }
	
    ob_start(); // Start output buffering
?>
    <div class="variant-motor-container">
        <div class="variant-motor-gallery">
            <div class="variant-motor-product-title">
                <h1><?php echo $model_title; ?></h1>
                <div class="variant-motor-dropdown">
                    <button class="variant-motor-change-model">เปลี่ยนรุ่นรถ</button>
                    <div class="variant-motor-dropdown-content">
                        <?php foreach ($filtered_variants as $variant): ?>
                            <a href="<?php echo $base_url . $variant->post_name; ?>"><?php echo $variant->post_title; ?></a>
                        <?php endforeach; ?>
                    </div>
                </div>
            </div>
            <div class="variant-motor-slider">
                <?php if (!empty($firstThreeImages)) : ?>
					<?php foreach ($firstThreeImages as $image) : ?>
						<div>
							<img 
								src="<?php echo esc_url($image['url']); ?>" 
								alt="<?php echo esc_attr($image['name']); ?>" 
								class="variant-motor-slider-img" 
							/>
						</div>
					<?php endforeach; ?>
				<?php else : ?>
					<p>No images available</p>
				<?php endif; ?>
            </div>
        </div>

        <div class="variant-motor-product-info">
            <div class="variant-motor-price-con">
                <div class="variant-motor-price">
                    <?php echo $price; ?>
                    <span class="variant-motor-monthly-price"><?php echo $monthly_payment; ?></span>
                </div>
                <div>
                    <button class="variant-motor-compare"><a href="<?php echo home_url('compare-motorcycles'); ?>">+ เช็คสเปค</a></button>
                </div>
            </div>
            <p class="variant-motor-subtitle">ราคา รถมอเตอร์ไซค์ <?php echo $variant_post_title; ?> ที่ไทย</p>
            <div>
                <div class="variant-motor-title">สเปค <?php echo $variant_post_title; ?></div>
                <div class="variant-motor-specs">
                    <div class="variant-motor-spec-pair">
                        <div class="variant-motor-specs-label">ประเภท</div>
                        <div class="variant-motor-specs-value"><?php echo $body_type; ?></div>
                    </div>
                    <div class="variant-motor-spec-pair">
                        <div class="variant-motor-specs-label">ความจุของเครื่องยนต์</div>
                        <div class="variant-motor-specs-value"><?php echo $capacity; ?></div>
                    </div>
                    <div class="variant-motor-spec-pair">
                        <div class="variant-motor-specs-label">กำลังไฟสูงสุด</div>
                        <div class="variant-motor-specs-value"><?php echo $maximum_power; ?></div>
                    </div>
                    <div class="variant-motor-spec-pair">
                        <div class="variant-motor-specs-label">ตัวเลือกการมีดเครื่องยนต์</div>
                        <div class="variant-motor-specs-value"><?php echo $engine_opening_option; ?></div>
                    </div>
                </div>
				<button class="variant-motor-cta-button"><a href="<?php echo $base_url . $variant->post_name; ?>">เช็คสเปค</a></button>
            </div>
        </div>
    </div>

    <style>
        .variant-motor-container {
            padding: 20px;
            display: flex;
            gap: 40px;
        }



        .variant-motor-gallery:hover .slick-arrow {
            opacity: 1;
        }


        .variant-motor-slider-img {
            width: 100%;
            height: 258px;
            object-fit: cover;
        }

        .variant-motor-gallery {
            width: 456px;
            height: 258px;
        }

        .variant-motor-slick-arrow {
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            background-color: rgba(0, 0, 0, 0.5);
            color: white;
            border: none;
            border-radius: 50%;
            width: 30px;
            height: 30px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
            cursor: pointer;
            opacity: 0;
            transition: opacity 0.3s ease-in-out;
            z-index: 1;
        }


        .variant-motor-prev {
            left: 10px;
        }


        .variant-motor-next {
            right: 10px;
        }


        .variant-motor-prev:before,
        .variant-motor-next:before {
            font-size: 16px;
            line-height: 1;
            opacity: 1;
            color: white;
        }


        .variant-motor-product-info {
            width: 100%;
        }


        .variant-motor-product-title {
            display: flex;
            align-items: center;
            margin-bottom: 20px;
            position: relative;
            gap: 20px;
        }


        .variant-motor-product-title h1 {
            margin: 0;
            font-size: 24px;
        }


        .variant-motor-dropdown {
            position: relative;
            display: inline-block;
        }


        .variant-motor-change-model {
            padding: 8px 16px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
        }


        .variant-motor-change-model::after {
            content: "";
            border-left: 5px solid transparent;
            border-right: 5px solid transparent;
            border-top: 5px solid #666;
            margin-left: 5px;
        }


        .variant-motor-dropdown-content {
            display: none;
            position: absolute;
            right: 0;
            background-color: white;
            min-width: 160px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
            border-radius: 4px;
            z-index: 1;
        }


        .variant-motor-dropdown-content a {
            color: black;
            padding: 12px 16px;
            text-decoration: none;
            display: block;
        }


        .variant-motor-dropdown-content a:hover {
            background-color: #f1f1f1;
        }


        .variant-motor-dropdown:hover .variant-motor-dropdown-content {
            display: block;
        }


        .variant-motor-price {
            font-size: 36px;
            color: #576b95;
            margin-bottom: 10px;
            font-family: "roboto";
            font-weight: 700;
        }


        .variant-motor-monthly-price {
            font-size: 18px;
            color: #576b95;
            line-height: 20px;
            display: inline-block;
            background: rgba(87, 107, 149, 0.1);
            padding: 5px 8px;
            margin-left: 8px;
            border-radius: 4px;
        }


        .variant-motor-subtitle {
            font-size: 14px;
            line-height: 14px;
            color: #8c8c8c;
            margin-top: 6px;
            font-family: "Roboto";
        }


        .variant-motor-specs {
            display: grid;
            grid-template-columns: auto 1fr auto 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }


        .variant-motor-specs dt {
            color: #666;
        }


        .variant-motor-specs dd {
            margin: 0;
            font-weight: 500;
        }


        .variant-motor-cta-button {
            display: block;
            width: 48%;
            padding: 12px;
            background: white;
            border: 1px solid #ffb400;
            border-radius: 4px;
            color: #ffb400;
            font-size: 16px;
            text-align: center;
            font-weight: 700;
            cursor: pointer;
        }

        .variant-motor-title {
            font-family: "Roboto";
            font-weight: 700;
            font-size: 16px;
            margin-bottom: 10px;
            color: #262626;
        }

        .variant-motor-specs {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0 20px;
            margin-bottom: 30px;
        }


        .variant-motor-spec-pair {
            display: flex;
            justify-content: space-between;
            gap: 20px;
            padding: 12px 0;
            border-bottom: 1px solid #f0f0f0;
        }


        .variant-motor-specs-label {
            font-size: 18px;
            line-height: 25px;
            color: #999;
            font-family: "Roboto";
        }


        .variant-motor-specs-value {
            font-size: 18px;
            line-height: 25px;
            color: #262626;
            font-family: "Roboto";
        }

        .variant-motor-price-con {
            display: flex;
            justify-content: space-between;
        }

        .variant-motor-compare {
            display: block;
            width: 100%;
            padding: 12px;
            background: white;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            color: #8c8c8c;
            font-size: 16px;
            text-align: center;
            font-weight: 700;
            cursor: pointer;
        }

        .variant-motor-compare a {
            text-decoration: none;
            color: #8c8c8c;
        }
    </style>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js"></script>
    <script>
        jQuery(document).ready(function($) {
            $(".variant-motor-slider").slick({
                dots: false,
                arrows: true,
                infinite: true,
                speed: 500,
                slidesToShow: 1,
                slidesToScroll: 1,
                autoplay: true,
                autoplaySpeed: 3000,
                prevArrow: '<button type="button" class="variant-motor-slick-arrow variant-motor-prev">❮</button>',
                nextArrow: '<button type="button" class="variant-motor-slick-arrow variant-motor-next">❯</button>',
            });
        });
    </script>
<?php
    return ob_get_clean(); // Return the buffered content
}

add_shortcode('motor_variant_specification', 'motor_variant_specification_shortcode');
