<?php
function initialize_gallery_slick_slider()
{
?>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            $('.top-cars-sedan-list').slick({
                slidesToShow: 3, // Shows 4 cars initially
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false, // Prevents looping
                cssEase: 'ease', // Smooth scrolling
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
                            slidesToShow: 1.2,
                            slidesToScroll: 1
                        }
                    },
                    {
                        breakpoint: 480,
                        settings: {
                            slidesToShow: 1.2,
                            slidesToScroll: 1
                        }
                    }
                ]
            });
        });
    </script>
<?php
}
add_action('wp_footer', 'initialize_gallery_slick_slider');

function top_cars_shortcode($atts)
{
    global $wpdb;

    $make = get_query_var('make');
    $model = get_query_var('model');
    $listing_name = $make . '-' . $model;

    $term = get_term_by('slug', $make, 'listing_make');
    $term_name = $term ?  $term->name : '';

    $sedan_cars = [];
    if ($term) {
        // Get listings where _listing_make matches term ID and state is 1
        $sedan_cars = get_posts(array(
            'post_type' => 'listing',
            'posts_per_page' => 5, // Limit to 5 results
            'meta_query' => array(
                'relation' => 'AND',
                array(
                    'key' => '_listing_make',
                    'value' => $term->term_id,
                    'compare' => '='
                ),
                array(
                    'key' => 'state',
                    'value' => 1,
                    'compare' => '='
                ),
                array(
                    'key' => 'sort',
                    'value' => array(0, 1, 2, 3, 4),
                    'compare' => 'IN',
                )
            ),
            'orderby' => 'sort',
            'order' => 'ASC'
        ));
    }
    // Start output buffering
    ob_start();
?>
    <h2 class="wa-title-text"><?php echo 'Xe ' . $term_name . ' hàng đầu' ?></h2>
    <?php if (!empty($sedan_cars)) : ?>
        <div class="top-cars-sedan-carousel">
            <ul class="top-cars-sedan-list">
                <?php foreach ($sedan_cars as $car_post) : ?>
                    <?php
                    // Set up post data for the current car listing
                    setup_postdata($car_post);

                    // Get car title, thumbnail GUID, and make terms
                    $car_title = get_the_title($car_post->ID); // Get the title for the listing
                    // Get the car makes
                    $listing_makes = wp_get_post_terms($car_post->ID, 'listing_make');
                    $make_names = wp_list_pluck($listing_makes, 'name');

                    $args = array(
                        'post_type' => 'variant',
                        'posts_per_page' => -1,
                        'meta_query' => array(
                            array(
                                'key' => 'model',
                                'value' => $car_post->ID,
                                'compare' => 'like',
                            ),
                        ),
                    );
                    $variants = new WP_Query($args);

                    $post_thumbnail_id = get_post_thumbnail_id($car_post->ID);
                    $thumbnail_post = get_post($post_thumbnail_id);
                    $guid = $thumbnail_post->guid;

                    if (!empty($variants->posts)) {
                        $highest_price = null;
                        $lowest_price = null;
                        foreach ($variants->posts as $variant) {
//                             if (get_post_meta($variant->ID, 'on_sale', true) == 'Yes' &&  get_post_meta($variant->ID, 'state', true) == 1) {
//                                 $price = get_post_meta($variant->ID, 'retail_price', true);
//                                 if ($price && $price > $highest_price) {
//                                     $highest_price = $price;
//                                 }
//                                 if ($price && ($price < $lowest_price || $lowest_price == 0)) {
//                                     $lowest_price = $price;
//                                 }
//                             }
							if (get_post_meta($variant->ID, 'state', true) == 1 && get_post_meta($variant->ID, 'on_sale', true) == 'Yes') {
								$price = (float)get_post_meta($variant->ID, 'retail_price', true);
								 // Skip if price is zero
								if ($price > 0) {
									$lowest_price = is_null($lowest_price) ? $price : min($lowest_price, $price);
									$highest_price = is_null($highest_price) ? $price : max($highest_price, $price);
								}
							}
                        }
							$price = !is_null($lowest_price) && !is_null($highest_price) ?
								($lowest_price === $highest_price ?
									format_price_vietnam($lowest_price) :
									format_price_vietnam($lowest_price) . ' - ' . format_price_vietnam($highest_price)
								) : 'Đang cập nhật';
                    } else {
                        $price = 'Đang cập nhật';
                    }
                    ?>
                    <li class="car-sedan-item">
                        <div class="car-thumbnail">
                            <span>
                                <img src="<?php echo $guid; ?>" alt="<?php echo esc_attr($car_post->post_title); ?>" class="fixed-thumbnail">
                            </span>
                        </div>

                        <div class="car-content">
                            <div class="car-info">
                                <p class="car-make"><?php echo esc_html(implode(', ', $make_names)); ?></p>
                            </div>
                            <div class="car-titile-con">
                                <a href="<?php echo get_permalink($car_post->ID); ?>" class="car-title"><?php echo esc_html($car_title); ?></a>

                            </div>
                            <div class="price">
                                <span><?php echo esc_html($price); ?></span>
                            </div>
                            <div class="view-model-button">
                                <a href="<?php echo get_permalink($car_post->ID); ?>">  Xem dòng xe  </a>
                            </div>
                        </div>
                    </li>
                <?php endforeach; ?>
            </ul>
            <style>
                .price {
                    font-size: 14px;
                    color: #576b95;
                    font-weight: 700;
                    margin-top: -5px;
					margin-bottom:10px;
					font-family:'Roboto';
                }

                .car-titile-con {
                    margin-top: 0px;
                    color: #262626;
                    font-weight: 700;
                    font-size: 16px;
                }

                .view-model-button {
                    padding: 8px 12px;
                    background-color: white;
                    color: #ffb400 !important;
                    text-decoration: none;
                    border: 1px solid #32D0C6;
                    border-radius: 4px;
                    margin-top: auto;
                    text-align: center;
                    width: 100% !important;
                    font-weight: 700;
                    transition: background-color 0.3s ease, color 0.3s ease;
					font-family:'Roboto';
                }
				.car-title{
					font-size:16px;
					color:#262626;
					font-family:'Roboto';
				}
				.view-model-button a {
			   color: #ffb400 !important;
				}

            


                /* Carousel container */
                .top-cars-sedan-list {
                    display: flex;
                    flex-wrap: nowrap;
                    /* Prevents wrapping onto a new line */
                    overflow: visible;
                    /* Hides any overflowed content */
                    position: relative;
                    gap: 20px;
                    margin-left: -33px;
                }



                .slick-prev:focus,
                .slick-next:focus,
                .slick-prev:active,
                .slick-next:active {
                    background-color: #ffffff !important;
                    /* Keep the same background color after click */
                    color: black !important;
                    /* Retain the arrow color */
                    outline: none;
                    /* Remove any default browser outlines */
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15);

                }

                .slick-prev:before {
                    content: '←';

                    color: black !important;
                }

                .slick-next:before {
                    content: '→';
                    color: black !important;
                }


                .slick-prev,
                .slick-next {
                    background-color: #ffffff !important;
                    /* Default white background */
                    border-radius: 50%;
                    width: 50px;
                    height: 50px;
                    z-index: 10;
                    position: absolute;
                    top: 50%;
                    transform: translateY(-50%);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
                    transition: background-color 0.3s ease, color 0.3s ease;
                    border: none;
                }

                .slick-prev:before,
                .slick-next:before {
                    font-size: 20px;
                    color: black !important;
                    /* Black arrow */
                }

                /* Hover state */
                .slick-prev:hover,
                .slick-next:hover {
                    background-color: white !important;
                    /* Change to yellow on hover */
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;

                }

                /* Focus state */
                .slick-prev:focus,
                .slick-next:focus {
                    background-color: white !important;
                    /* Keep background yellow on focus */
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;

                    outline: none;
                }

                /* Active state when button is clicked */
                .slick-prev:active,
                .slick-next:active {
                    background-color: white !important;
                    /* Keep background yellow when clicked */
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;

                }
                 .car-sedan-item {
                    display: flex;
                    flex-direction: column;
                    border: 1px solid #e0e0e0;
                    width: calc(250px - 30px);
                    border-radius: 8px;
                    overflow: hidden;
                    padding: 15px;
                    width: 250px;
                    margin: 0 0px;
                    position: relative;
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                }

				.top-cars-sedan-carousel .slick-track{
				    display: flex !important;
   					 gap: 20px !important;
				}
                /* Hover effect for car items */
                .car-item:hover {
                    /* transform: translateY(0.1px); */
                    box-shadow: 0 5px 5px rgba(0, 0, 0, 0.1);
                }
				.top-cars-sedan-carousel{
					margin-left:15px;
				}
                /* Thumbnail styling */
                .car-thumbnail {
                    position: relative;
                    overflow: hidden;
                }
			.top-cars-sedan-carousel .slick-prev {
					left: -6px !important;     
				}
				.top-cars-sedan-carousel .slick-next {
					right: -13px !important;
				}
                .car-thumbnail img {
                    width: 100%;
                    height: 140px;
                    transition: transform 0.3s ease;
                    object-fit: cover;
                }
	@media screen and (max-width: 768px) {
              .top-cars-sedan-carousel .slick-prev {
					display:none !important;     
				}
				.top-cars-sedan-carousel .slick-next {
					display:none !important;    
				}
				}
				@media (min-width: 768px) and (max-width: 1024px) {
.slick-prev {
    left: -2px !important;
}
}
            </style>
        </div>
    <?php endif; ?>

<?php
    // End output buffering and return the content
    return ob_get_clean();
}
add_shortcode('top_cars', 'top_cars_shortcode');
