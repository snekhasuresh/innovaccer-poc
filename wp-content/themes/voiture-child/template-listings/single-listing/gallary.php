<?php
function enqueue_overview_gallery_css()
{
    wp_enqueue_style('overview-gallery-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/gallery.css', array(), '1.0', 'all');
}

function car_image_gallery_shortcode()
{
    enqueue_overview_gallery_css();
    require_once get_stylesheet_directory() . '/json-ld/car-json-ld.php';

    if (!defined('ABSPATH')) {
        exit;
    }

    global $wpdb;

    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data || !is_array($global_listing_post_data)) {
        return;
    }

    $global_listing_post = $global_listing_post_data['post'];

    $post_id = $global_listing_post->ID;
    $post_title = $global_listing_post->post_title;
    $post_meta_data = $global_listing_post_data['post_meta'];

    $segment = $post_meta_data['listing-segment'][0];
    $body_type_id = $post_meta_data['_listing_type'][0];
    $body_type = get_term($body_type_id, 'listing_type');

    $variant_posts = $global_listing_post_data['variant_posts'];
    $variant_ids = array_map(function ($variant) {
        return $variant->ID;
    }, $variant_posts);

    $imagesdata = $global_listing_post_data['image_data'];

    // Fetch images based on listing ID if no variant images are found
    if (empty($imagesdata)) {
        $listing_gallery_query = $wpdb->prepare(
            "SELECT meta_value 
            FROM {$wpdb->postmeta} 
            WHERE post_id = %d 
            AND meta_key = '_listing_detail'",
            $post_id
        );
        $listing_gallery = $wpdb->get_var($listing_gallery_query);
		if($listing_gallery){
			$gallery_ids = explode(',', $listing_gallery);
			$gallery_ids = maybe_unserialize($listing_gallery);
			$listing_gallery = $gallery_ids;
		}
    }

    $price = $global_listing_post_data['price'] ?? '';

    $transmissions = array();
    $horsepower = 0;
    $capacity = 0;
    $seats = 0;
    $motor_output = 0;
    $ev_range = 0;
    $battery_capacity = 0;

    $variants_meta_data = $global_listing_post_data['variant_meta_data'] ?? '';
    foreach ($variant_ids as $variant_id) {
        $variant_meta = $variants_meta_data[$variant_id];

        if (isset($variant_meta['horsepower'][0])) {
            $horsepower = $variant_meta['horsepower'][0];
        }

        if (isset($variant_meta['capacity'][0])) {
            $capacity = $variant_meta['capacity'][0];
        }

        if (isset($variant_meta['fuel_type'][0])) {
            $fuel_type = $variant_meta['fuel_type'][0];
        }

        if (isset($variant_meta['transmission'][0])) {
            $transmissions[] = $variant_meta['transmission'][0];
        }
        if (isset($variant_meta['motor_output'][0])) {
            $motor_output = $variant_meta['motor_output'][0];
        }
        if (isset($variant_meta['ev_range'][0])) {
            $ev_range = $variant_meta['ev_range'][0];
        }
        if (isset($variant_meta['battery_capacity'][0])) {
            $battery_capacity = $variant_meta['battery_capacity'][0];
        }
    }

    $transmissions = array_unique($transmissions);
    // Transmission or Motor Output
    if (!empty($transmissions)) {
        $transmission_label = 'Hộp số';
        $transmission_display = implode(', ', $transmissions);
    } elseif (!empty($motor_output)) {
        $transmission_label = 'Motor Output';
        $transmission_display = $motor_output . ' PS';
    } else {
        $transmission_label = 'Hộp số';
        $transmission_display = '-';
    }

    // Capacity or EV Range
    if (!empty($capacity)) {
        $capacity_label = 'Dung tích';
        $capacity_display = $capacity . ' L';
    } elseif (!empty($ev_range)) {
        $capacity_label = 'EV Range';
        $capacity_display = $ev_range . ' km';
    } else {
        $capacity_label = 'Dung tích';
        $capacity_display = '- L';
    }

    // Horsepower or Battery Capacity
    if (!empty($horsepower)) {
        $horsepower_label = 'Công suất cực đại';
        $horsepower_display = $horsepower . ' PS';
    } elseif (!empty($battery_capacity)) {
        $horsepower_label = 'Batery Capacity';
        $horsepower_display = $battery_capacity . ' kWh';
    } else {
        $horsepower_label = 'Công suất cực đại';
        $horsepower_display = '- PS';
    }
    $specs = array(
		'Loại cơ thể' => $body_type->name,
        'phân đoạn' => $segment,
		$capacity_label => $capacity_display,
        $horsepower_label => $horsepower_display,
        $transmission_label => $transmission_display,
        'Loại năng lượng' => $fuel_type,
    );

    $make = $global_listing_post_data['listing_make_term']->name;
    $model = $global_listing_post->post_title;
    add_car_json_ld($make, $model, $specs);

    $segment_icon = wp_get_attachment_image_url(32256, 'Segment');
    $Body_Type_icon = wp_get_attachment_image_url(32254, 'body type');
    $Transmission = wp_get_attachment_image_url(32252, 'Transmission');
    $Capacity = wp_get_attachment_image_url(32253, 'Capacity');
    $Horsepower = wp_get_attachment_image_url(32259, 'horse power');
    $fuel_type = wp_get_attachment_image_url(32260, 'seat');


    $icons = [
		'Loại cơ thể' => $Body_Type_icon,
        'phân đoạn' => $segment_icon,
		$capacity_label     => $Capacity,
		$horsepower_label   => $Horsepower,
        $transmission_label => $Transmission,
        'Loại năng lượng'       => $fuel_type,
    ];

    ob_start();
?>
    <div class="container-gallary">
        <div class="row">
            <!-- Gallery Section (Left side) -->
            <div class="col-md-5">
                <div class="carousel-container" style="padding: 0px;">
                    <div class="carousel-main">
                        <div class="carousel-images">
                            <?php if (empty($imagesdata) && !empty($listing_gallery)) {
                                foreach ($listing_gallery as $index => $image): ?>
                                    <img src="<?php echo esc_url($image); ?>" alt="Image <?php echo $index; ?>" class="carousel-image">
                                <?php endforeach; ?>
                            <?php }
                            ?>
                            <?php foreach ($imagesdata as $imageData): ?>
                                <?php
                                // Decode the JSON data for image_data
                                $imagesArray = json_decode($imageData->image_data);
                                $latestImages = [];
                                if ($imagesArray) {
                                    foreach ($imagesArray as $image) {
                                        $latestImages[] = $image;
                                    }
                                }

                                if (is_array($latestImages)) {
                                    $latestImages = array_slice($latestImages, 0, 3);
                                } else {
                                    $latestImages = [];
                                }

                                foreach ($latestImages as $index => $image): ?>
                                    <img src="<?php echo esc_url($image->url); ?>" alt="Image <?php echo $index; ?>" class="carousel-image">
                                <?php endforeach; ?>
                            <?php endforeach; ?>
                        </div>
                        <button class="carousel-button prev">
                            <span class="carousel-icon">&#10094;</span>
                        </button>
                        <button class="carousel-button next">
                            <span class="carousel-icon">&#10095;</span>
                        </button>
                    </div>
                </div>

                <div class="carousel-thumbnails-gal">
                    <?php
                    // Initialize arrays to store images and count
                    $galleryImages = [
                        'Exterior' => null,
                        'Interior' => null,
                        'Others' => null,
                    ];
                    $totalImageCount = 0;

                    // Process the images data
                    foreach ($imagesdata as $image) {
                        $imageDataArray = json_decode($image->image_data);
                        if ($imageDataArray) {
                            foreach ($imageDataArray as $imgData) {
                                // Store one image for Interior and one for Exterior
                                if ($image->type === "Exterior" && $galleryImages['Exterior'] === null) {
                                    $galleryImages['Exterior'] = [
                                        "url" => $imgData->url,
                                        "alt" => "Exterior",
                                        "text" => " Ngoại thất"
                                    ];
                                } elseif ($image->type === "Interior" && $galleryImages['Interior'] === null) {
                                    $galleryImages['Interior'] = [
                                        "url" => $imgData->url,
                                        "alt" => "Interior",
                                        "text" => "Nội thất"
                                    ];
                                }
                                if ($image->type === "Others" && $galleryImages['Others'] === null) {
                                    $galleryImages['Others'] = [
                                        "url" => $imgData->url,
                                        "alt" => "Gallery",
                                        "text" => $totalImageCount++ . " hình ảnh",
                                    ];
                                    // $backgroundImageUrl = $imgData->url;
                                }
                                // Count all images regardless of type
                                $totalImageCount++;
                            }
                        }
                    }
					if ($galleryImages) {
                        $galleryImages[] = [
                            "url" => $imgData->url,
                            "alt" => "images",
                            "text" => $totalImageCount . " hình ảnh",
                        ];
                    }
                    // Display the images for Interior and Exterior
                    foreach ($galleryImages as $image):
                        if ($image): // Check if the image exists
                    ?>
                            <a href="<?php echo get_permalink($post_id) . 'hinh-anh/'; ?>" target="_blank" class="thumbnail-link">
                                <img src="<?php echo $image['url']; ?>" alt="<?php echo $image['alt']; ?>" class="thumbnail-gal-individual">
                                <span class="image-text"><?php echo $image['text']; ?></span> <!-- Text on the thumbnail -->
                            </a>
                    <?php
                        endif;
                    endforeach;
                    ?>
                </div>

            </div>

            <!-- Specs Section (Right side) -->
            <div class="col-md-7">
                <div style="margin-top: -10px;">
                    <div class="price-range"><?php echo $price; ?></div>

                    <span class="widget-title"><?php esc_html_e(' Thông số kỹ thuật '.$post_title, 'voiture'); ?></span>

                    <div class="specs-container">
                        <?php foreach ($specs as $key => $value) : ?>
                            <div class="spec-field">
                                <div class="icon">
                                    <!-- Check if the $key has an associated icon image -->
                                    <?php if (isset($icons[$key])): ?>
                                        <img src="<?php echo esc_url($icons[$key]); ?>" alt="<?php echo esc_attr($key); ?> Icon" width="30" height="30">
                                    <?php else: ?>
                                        <i class="fas fa-car-side"></i> <!-- Default FontAwesome icon if no match -->
                                    <?php endif; ?>
                                </div>
                                <div class="spec-label"><?php echo $key; ?></div>
                                <div class="spec-value"><?php echo $value; ?></div>
                            </div>
                        <?php endforeach; ?>



                    </div>

                    <div class="buttons-container-spec">
                        <button class="view-specs-button"><a href="<?php echo get_permalink($post_id) . 'thong-so-ky-thuat'; ?>"> Xem thông <?php echo $post_title ?></a></button>
<!--                         <button class="trade-in-button"><a href="<?php echo home_url('/book-test-drive') . '/?make=' . urlencode($make) . '&model=' . urlencode($model); ?>">ขายรถคันเดิมเพื่อแลกกับคันนี้ </a></button> -->
                    </div>

                </div>
            </div>
        </div>
    </div>

    <!-- Slick CSS -->
    <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/slick-carousel@1.8.1/slick/slick.css" />
    <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/slick-carousel@1.8.1/slick/slick-theme.css" />


    <!-- Slick JS -->
    <script type="text/javascript" src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/slick-carousel@1.8.1/slick/slick.min.js"></script>


    <script type="text/javascript">
        $(document).ready(function() {
            // Initialize the Slick slider
            $('.carousel-images').slick({
                infinite: true,
                slidesToShow: 1,
                slidesToScroll: 1,
                autoplay: true,
                autoplaySpeed: 3000,
                arrows: false
            });


            // Custom button functionality
            $('.carousel-button.prev').click(function() {
                $('.carousel-images').slick('slickPrev'); // Go to the previous slide
            });


            $('.carousel-button.next').click(function() {
                $('.carousel-images').slick('slickNext'); // Go to the next slide
            });
            $('.slick-track').css({
                'gap': '0px',
            });
        });
    </script>


<?php
    return ob_get_clean();
}
add_shortcode('car_image_gallery', 'car_image_gallery_shortcode');
