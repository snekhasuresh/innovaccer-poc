<?php
// import css from ./css/fuel-consumption-view-model.css
function fuel_consumption_view_model_styles()
{
    wp_enqueue_style('fuel-consumption-view-model-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/fuel-consumption-view-model.css', array(), '1.0', 'all');
}

add_shortcode('fuel_consumption_view_model', 'fuel_consumption_view_model_shortcode');
function fuel_consumption_view_model_shortcode()
{
    fuel_consumption_view_model_styles();

    ob_start();

    echo display_single_car_post();

    return ob_get_clean();
}

function display_single_car_post()
{
    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data) {
        return;
    }
    $listing_post = $global_listing_post_data['post'];
    if (!$listing_post) {
        return;
    }

    $image_guid = $global_listing_post_data['thumbnail'];
    $price = $global_listing_post_data['price'];
    $make_term_id = $global_listing_post_data['listing_make_term']->term_id;
    $post_id = $listing_post->ID;
    $post_title = $listing_post->post_title;
    $permalink = get_permalink($post_id);

    $base_url = get_home_url();
    $make = get_query_var('make');
    $base_url = $base_url . '/cars/' . $make . '/';

    $other_model_args = array(
        'post_type' => 'listing',
        'posts_per_page' => -1,
        'meta_query' => array(
            array(
                'key' => '_listing_make',
                'value' => $make_term_id, // Correct match with serialized data format
                'compare' => '=',
            ),
            array(
                'key' => 'state',
                'value' => 1,
                'compare' => '='
            )
        ),
    );
    $other_models = new WP_Query($other_model_args);
$translate = [
    
'Related model' => 'Dòng xe liên quan',
'View Model' => 'Xem dòng xe'

];


 echo '<h2 class="wa-title-text">' . $translate['Related model'] . '</h2>';
?>
    <div class="single-car">
        <div class="car-item">
            <a href="<?php echo esc_url($permalink); ?>" class="car-link">
                <div style="display:flex; justify-content: center;">
                    <img style="width: 100%; height: 150px; object-fit: cover;" src="<?php echo esc_url($image_guid); ?>"
                        alt="<?php echo esc_attr($post_title); ?>">
                </div>
                <span class="car-details">
                    <p class="car-make"><?php echo get_the_term_list($post_id, 'listing_make', '', ', '); ?></p>
                    <h4 class="car-title"><?php echo esc_html($post_title); ?></h4>
                </span>
                <span class="car-price">
                    <p><?php echo $price; ?></p>
                </span>
                <span class="car-button">
                    <a href="<?php echo esc_url($permalink); ?>" class="btn-view-model"> <?php echo $translate['View Model']; ?></a>
                </span>
            </a>
            <div class="car-variant-dropdown">
                <div class="variant-header" onclick="toggleVariants('<?php echo esc_js($post_id); ?>')">
                    <span class="variant-count"><?php echo count($other_models->posts) . '  dòng xe ' . ucfirst($make); ?> khác </span>
                    <button class="variant-toggle" data-id="<?php echo esc_attr($post_id); ?>">
                        <i class="fas fa-chevron-down"></i> <!-- Font Awesome down icon -->
                    </button>
                </div>

                <!-- Display variant list here -->
                <div>
                    <ul id="variant-list-<?php echo esc_attr($post_id); ?>" class="variant-list" style="display: none;">
                        <?php foreach ($other_models->posts as $other_model): ?>
                            <?php
                            $other_model_name = $other_model->post_name;
                            $other_model_slug = substr($other_model_name, strlen($make) + 1); // +1 to account for the hyphen
                            ?>
                            <li>
                                <a href="<?php echo $base_url . $other_model_slug; ?>">
                                    <?php echo $other_model->post_title; ?>
                                </a>
                            </li>
                        <?php endforeach; ?>
                    </ul>
                </div>
            </div>
        </div>

        <script>
            function toggleVariants(carID) {
                var variantList = document.getElementById('variant-list-' + carID);
                var toggleButton = document.querySelector('.variant-toggle[data-id="' + carID + '"]');

                // Check if the list is currently visible
                var isCurrentlyVisible = variantList.classList.contains('visible');

                // Hide all variant lists first
                var allVariantLists = document.querySelectorAll('.variant-list');
                allVariantLists.forEach(function(list) {
                    list.classList.remove('visible');
                });

                // Reset all toggle icons to down arrow
                var allToggleButtons = document.querySelectorAll('.variant-toggle');
                allToggleButtons.forEach(function(button) {
                    button.innerHTML = '<i class="fas fa-chevron-down"></i>'; // Reset to down arrow icon
                });

                // If the clicked list was not visible, show it
                if (!isCurrentlyVisible) {
                    variantList.classList.add('visible');
                    toggleButton.innerHTML = '<i class="fas fa-chevron-up"></i>'; // Change to up arrow icon
                }
            }
        </script>

    </div>
<?php
}
