<?php

function variant_gallery_carousel_shortcode()
{
    global $wpdb;

    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    $term = get_term_by('slug', $make, 'listing_make');
    $term_name = $term ?  $term->name : '';


    $model_posts = get_posts(array(
        'post_type' => 'listing',
        'meta_query' => array(
            array(
                'key' => '_listing_make',
                'value' => $term->term_id,
                'compare' => '='
            )
        ),
    ));

    if (!$model_posts) {
        return 'No models found for this make.';
    }

    $carDetails = [];

    foreach ($model_posts as $model) {

        // Get the first variant of the model
        $variant_id = $wpdb->get_var($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'variant' LIMIT 1",
            $model->ID
        ));

        if ($variant_id) {
            // Get images for the first variant of each model
            $images_sql = $wpdb->prepare(
                "SELECT type, image_data FROM car_image WHERE variant_post_id = %d",
                $variant_id
            );

            $imageResults = $wpdb->get_results($images_sql);

            $main_image = '';
            $small_images = [];
            $total_images = 0;

            foreach ($imageResults as $image) {
                $images = json_decode($image->image_data, true);

                if (!empty($images)) {
                    if ($image->type == 'Exterior' && empty($main_image)) {
                        $main_image = $images[0]['url'];
                    }

                    if ($image->type == 'Interior' || $image->type == 'Others') {
                        $small_images[] = $images[0]['url'];
                    }

                    $total_images += count($images);
                }
            }

            // Add model with its first variant's main and small images
            if (!empty($main_image)) {
                $carDetails[] = [
                    'main_image' => $main_image,
                    'small_images' => $small_images,
                    'caption' => $model->post_title,
                    'total_images' => $total_images
                ];
            }
        }
    }
    ob_start();
?>


    <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick.min.css" />
    <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.8.1/slick-theme.min.css" />


    <div class="container-filter-widget">
        <h2 class="wa-title-text"><?php echo $term_name; ?> Car Images</h3>
            <div class="row">
                <div class="">
                    <div class="carousel-filter-widget">
                        <?php foreach ($carDetails as $car): ?>
                            <div class="car-col ">
                                <a href="your-target-url-here" class="no-hover-style">
                                    <div class="car-image">
                                        <div class="main-image-container">
                                            <img src="<?php echo $car['main_image']; ?>" alt="<?php echo $car['caption']; ?>">
                                        </div>
                                        <div class="small-images">
                                            <?php foreach ($car['small_images'] as $small_image): ?>
                                                <div>
                                                    <img src="<?php echo $small_image; ?>" alt="<?php echo $car['caption']; ?>">
                                                </div>
                                            <?php endforeach; ?>
                                        </div>
                                    </div>
                                    <div class="car-caption">
                                        <div class="car-modal-name">
                                            <span><?php echo $car['caption']; ?></span>
                                        </div>
                                        <div class="total-count">
                                            <p><?php echo $car['total_images']; ?> Images &#10095;</p>
                                        </div>
                                    </div>
                                </a>
                            </div>
                        <?php endforeach; ?>
                    </div>
                </div>
            </div>
    </div>




    <style>
        .total-count {
            margin-top: -5px;
        }


        .no-hover-style {
            text-decoration: none;
            color: inherit;
        }

        .container-filter-widget .slick-track {
            display: flex;
            width: 4730px !important;
        }

        .no-hover-style:hover {
            color: inherit;
            text-decoration: none;
        }


        .car-caption {
            display: flex;
            justify-content: flex-start;
            flex-direction: column;
            align-items: flex-start;
            text-align: start;
            padding-left: 10px;
        }


        .car-modal-name {
            font-size: 18px;
            font-weight: 600;
        }


        .main-image-container {
            padding: 10px;
        }


        .main-image-container img {
            border-radius: 5px;
            width: 100%;
        }


        .small-images img {
          border-radius: 5px;
		  width: 217px !important;
  		  height: 131px;
        }


        .small-images {
            display: flex;
            gap: 10px;
            padding: 10px;
        }


        .car-col {
            margin: 10px;
            max-width: 45%;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }


        .car-col:hover {
            transform: translateY(-3px);
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
        }


        .carousel-filter-widget {
            max-width: 1200px;
            margin: auto;
            padding: 20px;
        }


        .slick-prev,
        .slick-next {
            background-color: rgba(0, 0, 0, 0.5);
            color: white;
            border: none;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            display: flex;
            justify-content: center;
            align-items: center;
            cursor: pointer;
            opacity: 1;
            z-index: 10;
            transition: background-color 0.3s;
        }


        .slick-prev:hover,
        .slick-next:hover {
            background-color: rgba(0, 0, 0, 0.8);
        }


        .slick-prev {
            left: -20px;
        }


        .slick-next {
            right: -20px;
        }


        .slick-slide {
            display: flex;
            justify-content: center;
        }
    </style>


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
            });
        });
    </script>


<?php
    return ob_get_clean();
}


add_shortcode('variant_gallery_carousel', 'variant_gallery_carousel_shortcode');
