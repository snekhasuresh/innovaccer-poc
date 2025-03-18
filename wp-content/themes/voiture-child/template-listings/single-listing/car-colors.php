<?php
if (!defined('ABSPATH')) {
    exit; // Exit if accessed directly
}
global $post;
global $wpdb;

// Check if the post type is 'listing'
if (get_post_type() == 'listing') {
?>
    <div>
        <h2 class="title"><?php echo esc_html(' Màu sắc' .$post->post_title); ?></h2>
        <div class="car-image-gallery">
            <?php if (have_rows('color_library')):
                $first_image_displayed = false; ?>
                <?php while (have_rows('color_library')):
                    the_row(); ?>
                    <?php
                    $color = get_sub_field('color');
                    $car_color_image = get_sub_field('car_color_image');

                    // Check if both color and car color image are available
                    if ($color && $car_color_image) {
                    ?>
                        <img src="<?php echo esc_url($car_color_image['url']); ?>" alt="<?php echo esc_attr($color); ?>"
                            class="car-image" data-color="<?php echo esc_attr($color); ?>"
                            style="display: <?php echo !$first_image_displayed ? 'block' : 'none'; ?>;">
                        <?php $first_image_displayed = true; ?>
                    <?php } ?>
                <?php endwhile; ?>
            <?php endif; ?>
        </div>

        <div class="color-options">
            <?php if (have_rows('color_library')): ?>
                <?php while (have_rows('color_library')):
                    the_row(); ?>
                    <?php
                    $color = get_sub_field('color');
                    $car_color_image = get_sub_field('car_color_image');

                    // Check if both color and car color image are available
                    if ($color && $car_color_image) {
                    ?>
                        <button class="color-select" data-color="<?php echo esc_attr($color); ?>"
                            style="background-color: <?php echo esc_attr($color); ?>;"></button>
                    <?php } ?>
                <?php endwhile; ?>
            <?php endif; ?>
        </div>
    </div>


    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // Handle color selection
            document.querySelectorAll('.color-select').forEach(function(button) {
                button.addEventListener('click', function() {
                    var selectedColor = this.getAttribute('data-color');

                    // Hide all car images
                    document.querySelectorAll('.car-image').forEach(function(image) {
                        image.style.display = 'none';
                    });

                    // Show the image that matches the selected color
                    var selectedImage = document.querySelector('.car-image[data-color="' + selectedColor + '"]');
                    if (selectedImage) {
                        selectedImage.style.display = 'block';
                    }
                });
            });
        });
    </script>

    <!-- <style>
        .car-image-gallery {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-bottom: 20px;
        }

        .car-image {
            max-width: 50%;
            height: auto;
            display: none;
        }

        .color-options {
            display: flex;
            justify-content: center;
            margin-top: 10px;
        }

        .color-select {
            width: 30px;
            height: 30px;
            border: 2px solid #ccc;
            border-radius: 50%;
            cursor: pointer;
            margin: 0 5px;
            outline: none;
            transition: transform 0.2s, border-color 0.2s;
        }

        .color-select:hover,
        .color-select:focus {
            transform: scale(1.1);
            border-color: #000;
        }
    </style> -->
<?php
}
?>