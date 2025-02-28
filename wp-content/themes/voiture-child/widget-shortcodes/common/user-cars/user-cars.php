<?php

function display_user_cars_with_popup_shortcode()
{
    // Enqueue CSS for the popup
    wp_enqueue_style(
        'user-cars-popup-styles',
        get_stylesheet_directory_uri() . '/widget-shortcodes/common/user-cars/user-cars.css',
        array(),
        '1.0.0'
    );

    // Enqueue JavaScript for the popup functionality
    wp_enqueue_script(
        'user-cars-popup-script',
        get_stylesheet_directory_uri() . '/widget-shortcodes/common/user-cars/user-cars.js',
        array('jquery'),
        '1.0.0',
        true
    );

    // Example user car data
    $user_cars_data = [
        [
            'image' => 'https://via.placeholder.com/150', // Placeholder image
            'variantId' => 1,
            'title' => '2023 Audi Q2 S-Line',
            'numberPlate' => 'QWWER',
        ],
        [
            'image' => 'https://via.placeholder.com/150', // Placeholder image
            'variantId' => 2,
            'title' => '2022 Audi Q2 S-Line',
            'numberPlate' => 'QWWR',
        ],
    ];

    ob_start();
    $token = $_COOKIE["wapcar_token"];

    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);
    if ($response != false) {
        // get user_cars from user meta
        $user_cars = get_user_meta($response['user_id'], 'user_cars', true);
        if (is_array($user_cars)) {
            $user_cars_data = [];
            foreach ($user_cars as $car) {
                $variantId = $car['variantId'];
                $numberPlate = $car['numberPlate'];

                // get variant post of ID in [variantId]
                $args = array(
                    'post_type' => 'variant',
                    'posts_per_page' => 1,
                    'p' => $variantId
                );
                $query = new WP_Query($args);
                if ($query->have_posts()) {
                    while ($query->have_posts()) {
                        $query->the_post();

                        $post_thumbnail_id = get_post_thumbnail_id(get_the_ID());
                        $thumbnail_post = get_post($post_thumbnail_id);
                        $guid = $thumbnail_post->guid;

                        $variant_data = [];
                        $variant_data['variantId'] = get_the_ID();
                        $variant_data['title'] = get_the_title();
                        $variant_data['numberPlate'] = $numberPlate;
                        $variant_data['image'] = $guid;
                        $user_cars_data[] = $variant_data;
                    }
                }

                wp_reset_postdata();
            }

            foreach ($user_cars_data as $car) {
?>
                <!-- Car Card -->
                <div class="car-card">
                    <div class="car-image">
                        <img src="<?php echo esc_url($car['image']); ?>" alt="<?php echo esc_attr($car['title']); ?>">
                    </div>
                    <div class="car-info">
                        <h3 class="car-title"><?php echo esc_html($car['title']); ?></h3>
                        <div class="number-plate"><?php echo esc_html($car['numberPlate']); ?></div>
                    </div>
                    <div class="car-actions">
                        <button class="edit-car-btn" data-car-image="<?php echo esc_attr($car['image']); ?>" data-car-variant-id="<?php echo esc_attr($car['variantId']); ?>" data-car-title="<?php echo esc_attr($car['title']); ?>" data-number-plate="<?php echo esc_attr($car['numberPlate']); ?>">
                            <i class="fa fa-edit"></i> Edit Car
                        </button>
                        <button class="delete-car-btn" data-car-variant-id="<?php echo esc_attr($car['variantId']); ?>">
                            <i class="fa fa-trash"></i>
                        </button>
                        <!-- <button type="button" class="delete-btn"><i class="fa fa-trash"></i> Delete</button> -->
                    </div>
                </div>

                <!-- Modal Popup -->
                <div id="car-edit-popup" class="popup">
                    <div class="popup-content">
                        <div class="popup-header">
                            <h3>Edit My Car</h3>
                            <button class="close-popup">&times;</button>
                        </div>
                        <div class="popup-body">
                            <div class="car-preview">
                                <img src="https://via.placeholder.com/150" alt="Car">
                                <h4>2023 Audi Q2 S-Line</h4>
                            </div>


                            <label for="license-plate">License Plate <span>*</span></label>
                            <input type="text" id="license-plate" name="license_plate" value="QWWER" required>
                            <!-- <button type="button" class="delete-btn"><i class="fa fa-trash"></i> Delete</button> -->
                            <button class="edit-confirm-btn" data-car-variant-id="<?php echo esc_attr($car['variantId']); ?>">Confirm</button>
                        </div>
                    </div>
                </div>

<?php
            }
        }
    }

    return ob_get_clean();
}
add_shortcode('user_cars', 'display_user_cars_with_popup_shortcode');


// action: delete_car
function delete_car()
{
    $token = $_COOKIE["wapcar_token"];
    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);

    if ($response == false) {
        wp_send_json_error(['message' => 'Invalid or expired OTP']);
    }

    $user_id = $response['user_id'];
    $variantId = $_POST['variantId'];
    $user_cars = get_user_meta($user_id, 'user_cars', true);
    if (is_array($user_cars)) {
        foreach ($user_cars as $key => $car) {
            if ($car['variantId'] == $variantId) {
                unset($user_cars[$key]);
                break;
            }
        }
        update_user_meta($user_id, 'user_cars', $user_cars);
    }

    wp_send_json_success(['message' => 'Car deleted successfully.']);
}
add_action('wp_ajax_delete_car', 'delete_car');
add_action('wp_ajax_nopriv_delete_car', 'delete_car');



// action: edit_car
function edit_car()
{
    $token = $_COOKIE["wapcar_token"];
    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);
    if ($response == false) {
        wp_send_json_error(['message' => 'Invalid or expired Token']);
    }

    $user_id = $response['user_id'];
    $variantId = $_POST['variantId'];
    $user_cars = get_user_meta($user_id, 'user_cars', true);
    if (is_array($user_cars)) {
        foreach ($user_cars as $key => $car) {
            if ($car['variantId'] == $variantId) {
                $user_cars[$key]['numberPlate'] = $_POST['numberPlate'];
                break;
            }
        }
        update_user_meta($user_id, 'user_cars', $user_cars);
    }

    wp_send_json_success(['message' => 'Car edited successfully.']);
}
add_action('wp_ajax_edit_car', 'edit_car');
add_action('wp_ajax_nopriv_edit_car', 'edit_car');
