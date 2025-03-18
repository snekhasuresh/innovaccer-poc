<?php
// import car-color.css
function enqueue_single_listing_car_color_css()
{
    wp_enqueue_style('car-color', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/car-color.css');
}

function get_car_colors_data()
{
    enqueue_single_listing_car_color_css();

    $global_listing_post_data = get_listing_from_query_vars();
    if (empty($global_listing_post_data) || !array($global_listing_post_data)) {
        return;
    }

    $listing_post = $global_listing_post_data['post'];
    $post_id = $listing_post->ID;
    $post_title = $listing_post->post_title;
    $default_car_image = $global_listing_post_data['thumbnail'];

    // Initialize a flag to check if there are car color images
    $has_images = false;

    $colors = get_field('color_library', $post_id);
    $car_colors_data = [];
    foreach ($colors as $color) {
        $car_colors_data[] = [
            'color' => $color['color'],
            'color_name' => $color['color_name'],
            'car_color_image' => $color['car_color_image']
        ];

        if ($color['car_color_image']) {
            $has_images = true;
        }
    }

    if (!$has_images) {
        return;
    }

    // check if it is car colors page
    $is_car_colors_page = is_page('colors');
?>
    <div>
        <div class=" individual-color-title-con">
            <?php if ($is_car_colors_page) : ?>
                <h1 class="individual-color-title"><?php echo esc_html(' Màu sắc ' .$post_title  ); ?></h1>
            <?php else : ?>
                <span class="individual-color-title"><?php echo esc_html( ' Màu sắc ' .$post_title ); ?></span>
            <?php endif; ?>
        </div>
        <div class="main-color-container">

            <!-- Slick slider container -->
            <div class="car-image-gallery slider">
                <div>
                    <img src="<?php echo $default_car_image; ?>" alt="<?php echo esc_attr($post_title); ?>"
                        class="car-image" data-color="default">
                </div>

                <?php foreach ($car_colors_data as $car_color_data) : ?>
                    <?php
                    $color = $car_color_data['color'];
                    $color_name = $car_color_data['color_name'];
                    $car_color_image = $car_color_data['car_color_image'];
                    if (!empty($car_color_image) && is_array($car_color_image) && isset($car_color_image['ID'])) {
                        $image_data = get_post($car_color_image['ID']);
                        if (!$image_data) {
                        }
                    } else {
                        $image_data = false;
                    }

                    if ($color && $car_color_image) {
                    ?>
                        <div>
                            <img src="<?php echo esc_url($image_data->guid); ?>" alt="<?php echo esc_attr($color); ?>"
                                class="car-image" data-color="<?php echo esc_attr($color); ?>">
                        </div>
                    <?php } ?>
                <?php endforeach; ?>
            </div>

            <div class='selected-color-name'>

            </div>

            <!-- Color options buttons -->
            <div class="color-options">
                <?php foreach ($car_colors_data as $car_color_data) : ?>
                    <?php
                    $color = $car_color_data['color'];
                    $color_name = $car_color_data['color_name'];
                    $car_color_image = $car_color_data['car_color_image'];

                    if ($color && $car_color_image) : ?>
                        <button
                            class="color-select"
                            data-color="<?php echo esc_attr($color); ?>"
                            data-color-name="<?php echo esc_attr($color_name); ?>"
                            style="background-color: <?php echo esc_attr($color); ?>;">
                        </button>
                    <?php endif ?>
                <?php endforeach; ?>
            </div>
			
        </div>
<!-- 		  <div class="view-more-container">
            <button class="view-more" data-offset="9"  ?>">
                ดูเพิ่มเติม
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
            </button>
        </div> -->
    </div>
<style>
	   .view-more-container {
        display: flex;
        justify-content: center;
        margin-top: 15px;
    }
</style>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // Initialize Slick Slider
            jQuery('.car-image-gallery').slick({
                slidesToShow: 1,
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="custom-prev custom-arrow" aria-label="Previous" type="button">&#10094;</button>',
                nextArrow: '<button class="custom-next custom-arrow" aria-label="Next" type="button">&#10095;</button>',
                fade: true,
                adaptiveHeight: true,
                infinite: false
            });

            // get the first color and set the image, color name
            var firstColor = document.querySelector('.color-select');
            if (firstColor) {
                firstColor.classList.add('active');
                var selectedColor = firstColor.getAttribute('data-color');
                var selectedIndex = [...document.querySelectorAll('.car-image')].findIndex(function(image) {
                    return image.getAttribute('data-color') === selectedColor;
                });
                if (selectedIndex !== -1) {
                    jQuery('.car-image-gallery').slick('slickGoTo', selectedIndex);
                }
                document.querySelector('.selected-color-name').innerHTML = firstColor.getAttribute('data-color-name');
            }


            // Color selection event
            document.querySelectorAll('.color-select').forEach(function(button) {
                button.addEventListener('click', function() {
                    var selectedColor = this.getAttribute('data-color');
                    var selectedIndex = [...document.querySelectorAll('.car-image')].findIndex(function(image) {
                        return image.getAttribute('data-color') === selectedColor;
                    });
                    if (selectedIndex !== -1) {
                        jQuery('.car-image-gallery').slick('slickGoTo', selectedIndex);
                    }

                    // change the color name
                    document.querySelector('.selected-color-name').innerHTML = this.getAttribute('data-color-name');
                });
            });

            // Update active button on slide change
            jQuery('.car-image-gallery').on('afterChange', function(event, slick, currentSlide) {
                var currentImage = slick.$slides.get(currentSlide).querySelector('.car-image');
                var currentColor = currentImage.getAttribute('data-color');
                document.querySelectorAll('.color-select').forEach(function(button) {
                    button.classList.remove('active');
                });
                var activeButton = document.querySelector('.color-select[data-color="' + currentColor + '"]');
                if (activeButton) {
                    activeButton.classList.add('active');
                }

                // change the color name
                document.querySelector('.selected-color-name').innerHTML = activeButton.getAttribute('data-color-name');
            });
        });
    </script>
<?php
}
add_shortcode("single_listing_car_color", "get_car_colors_data")
?>