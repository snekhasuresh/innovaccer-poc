<?php

function enqueue_motor_gallery_carousal_css()
{
    wp_enqueue_style('listing-gallery-carousel-style', get_stylesheet_directory_uri() . '/Gallary/css/cars-gallery-carousal.css', array(), '1.0', 'all');
}

function motor_filter_widget()
{
    enqueue_motor_gallery_carousal_css();

    global $wpdb;

    $global_listing_post_data = get_motor_listing_from_query_vars();
    $listing_make = $global_listing_post_data['listing_make_term']->name;
    $listing_make_id = $global_listing_post_data['listing_make_term']->term_id;


    $related_model_posts = get_posts(array(
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => -1,
        'meta_query' => array(
            array(
                'key' => 'make',
                'value' => $listing_make_id,
                'compare' => '='
            ),
            array(
                'key' => 'state',
                'value' => 1,
                'compare' => '='
            )
        ),
    ));

    if (!$related_model_posts) {
        return;
    }

    $carDetails = [];

    foreach ($related_model_posts as $model) {
        $model_permalink = get_permalink($model->ID);
        $model_gallery_permalink = $model_permalink . 'gallery/';
        // Get all variants of the model
        $variant_ids = $wpdb->get_col($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'motorcycle-variant'",
            $model->ID
        ));

        if (!empty($variant_ids)) {
            $main_image = '';
            $small_images = [];
            $total_images = 0;

            // Iterate through all variant IDs
            foreach ($variant_ids as $variant_id) {
                // Get images for the current variant
                $images_sql = $wpdb->prepare(
                    "SELECT type, image_data FROM car_image WHERE variant_post_id = %d",
                    $variant_id
                );

                $imageResults = $wpdb->get_results($images_sql);

                foreach ($imageResults as $image) {
                    $images = json_decode($image->image_data, true);

                    if (!empty($images)) {
                        if ($image->type == 'Exterior' && empty($main_image)) {
                            $main_image = $images[0]['url']; // First exterior image
                        }

                        // Collect small images, giving preference to Interior and Colour types
						if ($image->type == 'Interior' || $image->type == 'Colour') {
							foreach ($images as $img) {
								if (count($small_images) < 2) { // Ensure we only collect up to 2 small images
									$small_images[] = $img['url'];
								} else {
									break;
								}
							}
						}

                        $total_images += count($images);
                    }
                }
            }

            // Add model with its variants' images
            if (!empty($main_image)) {
                $carDetails[] = [
                    'main_image' => $main_image,
                    'small_images' => $small_images,
                    'caption' => 'รูปภาพ ' .$model->post_title,
                    'total_images' => $total_images
                ];
            }
        }
    }

    ob_start();
?>

    <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css" />
    <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css" />

    <div class="gallary-car-carousal-con ">
        <div class="container-filter-widget">
            <h2 class="wa-title-text">รูปภาพมอเตอร์ไซค์ <?php echo $listing_make; ?></h3>
                <div class="row">
                    <div class="">
                        <div class="carousel-filter-widget">
                            <?php foreach ($carDetails as $car): ?>
                                <div class="car-col ">
                                    <a href="<?php echo $model_gallery_permalink; ?>" class="no-hover-style">
                                        <div class="car-image-gal-carousal">
                                            <div class="main-image-container">
                                                <img src="<?php echo $car['main_image']; ?>" alt="<?php echo $car['caption']; ?>">
                                            </div>
                                            <div class="small-images">
                                                <?php $counter = 0; ?>
                                                <?php foreach ($car['small_images'] as $small_image): if ($counter >= 2) break; ?>
                                                    <div>
                                                        <img src="<?php echo $small_image; ?>" alt="<?php echo $car['caption']; ?>">
                                                    </div>
                                                <?php
                                                    $counter++;
                                                endforeach; ?>
                                            </div>
                                        </div>
                                        <div class="car-caption">
                                            <div class="car-modal-name">
                                                <span><?php echo $car['caption']; ?></span>
                                            </div>
                                            <div class="total-count">
                                                <p><?php echo $car['total_images']; ?> รูปภาพ &#10095;</p>
                                            </div>
                                        </div>
                                    </a>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    </div>
                </div>
        </div>
    </div>

    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.js"></script>
    <script>
        $(document).ready(function() {
            $('.carousel-filter-widget').slick({
                infinite: true,
                speed: 500,
                slidesToShow: 2,
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button type="button" class="slick-prev">Previous</button>',
                nextArrow: '<button type="button" class="slick-next">Next</button>',
                responsive: [{
                        breakpoint: 1030,
                        settings: {
                            slidesToShow: 2,
                            slidesToScroll: 1
                        }
                    },
                    {
                        breakpoint: 768,
                        settings: {
                            slidesToShow: 1,
                            slidesToScroll: 1
                        }
                    },
                ]
            });
        });
    </script>
<?php
    return ob_get_clean();
}


add_shortcode('motor_filter_widget', 'motor_filter_widget');
