<?php
function recommended_cars_shortcode_only()
{
    // Get the top car model data from the 'top_car_models' option
    $top_car_model_data = get_option('top_car_models', []);

    $top_car_model_ids = [];

    // Extract the car model IDs from the option data
    if (!empty($top_car_model_data) && is_array($top_car_model_data)) {
        foreach ($top_car_model_data as $type => $data) {
            if (isset($data['car_models']) && is_array($data['car_models'])) {
                foreach ($data['car_models'] as $car_model) {
                    if (isset($car_model['id'])) {
                        $top_car_model_ids[] = $car_model['id']; // Add car model ID to the array
                    }
                }
            }
        }
    }

    // Query to get the recommended cars based on the car model IDs
    $recommended_cars_query = new WP_Query(array(
        'post_type' => 'listing',
        'posts_per_page' => 5,
        'post__in' => $top_car_model_ids,
        'orderby' => 'post__in'
    ));

    if ($recommended_cars_query->have_posts()) :
        ob_start(); // Start output buffering to capture HTML output
?>
        <div class="recommended-cars-widget">
            <h2 style="margin-bottom: 16px;" class="wa-title-text">Các mẫu xe đề xuất</h2>
            <ul style="list-style: none; padding: 0;">
                <?php while ($recommended_cars_query->have_posts()) : $recommended_cars_query->the_post(); ?>
                    <li style="display: flex; padding: 12px 0; border-bottom: 1px solid #e0e0e0;">
                        <!-- Car Image -->
                        <div class="car-thumbnail">
                            <?php if (has_post_thumbnail()) : ?>
                                <?php the_post_thumbnail('thumbnail', ['style' => 'width: 100px; height: 56px; border-radius: 5px;']); ?>
                            <?php endif; ?>
                        </div>
                        <!-- Car Info -->
                        <div class="car-info" style="margin-left: 12px;">
                            <h3 style="font-size: 14px; font-weight: bold; margin: 0;">
                                <?php the_title(); ?>
                            </h3>
                            <p style="font-size: 12px; color: #576B95; margin: 5px 0 0;">
                                <?php echo get_post_meta(get_the_ID(), 'price', true); ?>
                            </p>
                        </div>
                    </li>
                <?php endwhile; ?>
            </ul>
        </div>
<?php
        wp_reset_postdata(); // Reset the post data
        return ob_get_clean(); // Return the buffered content
    else :
        return '<p>No recommended cars found.</p>';
    endif;
}
add_shortcode('recommended_cars_only', 'recommended_cars_shortcode_only');
