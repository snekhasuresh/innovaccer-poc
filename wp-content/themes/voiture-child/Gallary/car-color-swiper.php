<?php
if (!defined('ABSPATH')) {
    exit; // Exit if accessed directly
}

function gallery_color_gallery_shortcode()
{
    global $post; // Declare the post variable globally

    // Check if the post type is 'listing'
    if (get_post_type($post) === 'listing') {
        ob_start(); // Start output buffering

        // Static data for colors and images
        $static_colors = [
            [
                'color' => '#FF5733', // Example color
                'image' => 'https://images.wapcar.my/file1/4f82faa16a4d4f6e9ef3b3266e906270_1200.jpg', // Actual image URL
            ],
            [
                'color' => '#33FF57', // Example color
                'image' => 'https://images.wapcar.my/file1/5a7f2a71c1ee47f493f1428cfadfd31b_1200.jpg', // Actual image URL
            ],
            [
                'color' => '#3357FF', // Example color
                'image' => 'https://images.wapcar.my/file1/2f7f2a41c1ee47f493f1428cfadfd31b_1200.jpg', // Actual image URL
            ],
        ];
?>
        <div>
            <h2 class="gallery-title"><?php echo esc_html($post->post_title . ' Colors'); ?></h2>
            <div class="gallery-car-image-gallery">
                <?php if (!empty($static_colors)):
                    $first_image_displayed = false; ?>
                    <?php foreach ($static_colors as $item): ?>
                        <?php
                        $color = $item['color'];
                        $car_color_image = $item['image'];
                        ?>
                        <img src="<?php echo esc_url($car_color_image); ?>" alt="<?php echo esc_attr($color); ?>"
                            class="gallery-car-image" data-color="<?php echo esc_attr($color); ?>"
                            style="display: <?php echo !$first_image_displayed ? 'block' : 'none'; ?>;">
                        <?php $first_image_displayed = true; ?>
                    <?php endforeach; ?>
                <?php else: ?>
                    <p class="gallery-no-colors">No colors available for this listing.</p>
                <?php endif; ?>
            </div>

            <div class="gallery-color-options">
                <?php if (!empty($static_colors)): ?>
                    <?php foreach ($static_colors as $item): ?>
                        <?php
                        $color = $item['color'];
                        ?>
                        <button class="gallery-color-select" data-color="<?php echo esc_attr($color); ?>"
                            style="background-color: <?php echo esc_attr($color); ?>;"></button>
                    <?php endforeach; ?>
                <?php else: ?>
                    <p class="gallery-no-colors">No color options available.</p>
                <?php endif; ?>
            </div>
        </div>

        <script>
            document.addEventListener('DOMContentLoaded', function() {
                // Handle color selection
                document.querySelectorAll('.gallery-color-select').forEach(function(button) {
                    button.addEventListener('click', function() {
                        var selectedColor = this.getAttribute('data-color');

                        // Hide all car images
                        document.querySelectorAll('.gallery-car-image').forEach(function(image) {
                            image.style.display = 'none';
                        });

                        // Show the image that matches the selected color
                        var selectedImage = document.querySelector('.gallery-car-image[data-color="' + selectedColor + '"]');
                        if (selectedImage) {
                            selectedImage.style.display = 'block';
                        }
                    });
                });
            });
        </script>

        <style>
            .gallery-car-image-gallery {
                display: flex;
                justify-content: center;
                align-items: center;
                margin-bottom: 20px;
            }

            .gallery-car-image {
                max-width: 50%;
                height: auto;
                display: none;
            }

            .gallery-color-options {
                display: flex;
                justify-content: center;
                margin-top: 10px;
            }

            .gallery-color-select {
                width: 30px;
                height: 30px;
                border: 2px solid #ccc;
                border-radius: 50%;
                cursor: pointer;
                margin: 0 5px;
                outline: none;
                transition: transform 0.2s, border-color 0.2s;
            }

            .gallery-color-select:hover,
            .gallery-color-select:focus {
                transform: scale(1.1);
                border-color: #000;
            }

            .gallery-no-colors {
                text-align: center;
                color: #777;
                font-size: 16px;
                margin-top: 10px;
            }
        </style>
<?php
        return ob_get_clean(); // Return the buffered content
    }

    return ''; // Return empty if not a listing post type
}

// Register the shortcode
add_shortcode('gallery_color_gallery', 'gallery_color_gallery_shortcode');
?>