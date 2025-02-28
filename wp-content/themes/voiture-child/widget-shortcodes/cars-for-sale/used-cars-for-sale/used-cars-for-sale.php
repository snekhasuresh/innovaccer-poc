<?php
add_shortcode('used_cars_for_sale', 'used_cars_for_sale_shortcode');

function used_cars_for_sale_shortcode()
{
    wp_enqueue_style('font-awesome', 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.7.1/css/all.min.css');

    $make = get_query_var('make');
    $model = get_query_var('model');
    $used_cars = get_used_cars($make, $model)['data'];
    if (empty($used_cars)) {
        return;
    }
    $used_cars = array_slice($used_cars, 0, 6);

    foreach ($used_cars as $key => $car) {
        $used_cars[$key]['images'] = json_decode($car['images'], true);
    }

    ob_start();
?>
    <h2 class="wa-title-text">Used Cars For Sale</h2>
    <div class="car-listing-container-used-car">
        <div class="car-listing-container">
            <div class="car-grid">
                <?php foreach ($used_cars as $car) {
                    echo user_car_listing_single($car);
                } ?>
            </div>
            <div class="view-more">
                <a href="#" class="view-more-link">View More <span class="arrow">›</span></a>
            </div>
        </div>
    </div>

    <style>
        .car-listing-container-used-car .car-listing-container {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
        }

        .car-listing-container-used-car .car-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 30px;
            margin-bottom: 30px;
        }

        .car-listing-container-used-car .car-listing-card {
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s;
            display: flex;
            flex-direction: column;
        }

        .car-listing-container-used-car .car-listing-card:hover {
            transform: translateY(-5px);
        }

        .car-listing-container-used-car .car-image-container {
            position: relative;
            width: 100%;
            background: white;
            height: 190px;
        }

        .car-listing-container-used-car .car-image {
            width: 100%;
            height: auto;
            display: block;
            object-fit: cover;
            height: 190px !important;
            margin: 0 auto 0px !important;
        }

        .car-listing-container-used-car .car-details {
            padding: 20px;
            flex-grow: 1;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }

        .car-listing-container-used-car .car-title {
            font-size: 16px;
            font-weight: 600;
            margin: 0;
            color: #262626;
            line-height: 1.4;
            display: -webkit-box;
            -webkit-line-clamp: 1;
            -webkit-box-orient: vertical;
            overflow: hidden;
            text-overflow: ellipsis;
            font-family: "Roboto";
            margin-top: -24px;
        }

        .car-listing-container-used-car .car-price {
            margin-bottom: 10px;
            font-size: 14px;
            font-weight: bold;
            color: #576b95;
            font-family: "Roboto";
            margin-top: -12px !important;
        }

        .view-more {
            text-align: center;
            margin-top: 30px;
        }

        .view-more-link {
            display: inline-block;
            color: #666;
            text-decoration: none;
            font-size: 16px;
            margin-bottom: 10px;
        }

        .arrow {
            font-size: 18px;
            margin-left: 5px;
        }

        .wapcar-link {
            color: #ff4444;
            font-size: 18px;
            font-weight: bold;
        }

        @media (max-width: 768px) {
            .car-grid {
                grid-template-columns: repeat(1, 1fr);

            }

            .car-listing-container-used-car .car-image {
                width: 100%;
                height: auto;
                display: block;
                object-fit: cover;
                height: 190px !important;
                margin: 0 auto 0px;
            }

            .car-listing-container-used-car .car-title {
                font-size: 16px;
                font-weight: 600;
                margin: 0;
                color: #262626;
                line-height: 1.4;
                display: -webkit-box !important;
                -webkit-line-clamp: 1 !important;
                -webkit-box-orient: vertical !important;
                overflow: hidden !important;
                text-overflow: ellipsis !important;
                font-family: "Roboto" !important;
                margin-top: -24px !important;
            }

            .car-listing-container-used-car .car-price {
                margin-bottom: 10px;
                font-size: 14px;
                font-weight: bold;
                color: #576b95;
                font-family: "Roboto";
                margin-top: -12px !important;
            }

            .car-listing-container-used-car .car-listing-card {
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                transition: transform 0.2s;
                display: flex;
                flex-direction: column;
                height: 251px !important;
            }

            .car-listing-container-used-car .car-grid {
                display: grid;
                grid-template-columns: repeat(1, 1fr) !important;
                gap: 30px;
                margin-bottom: 30px;
            }
        }

        @media (min-width: 768px) and (max-width: 1024px) {
            .car-listing-container-used-car .car-grid {
                display: grid;
                grid-template-columns: repeat(2, 1fr) !important;
                gap: 30px;
                margin-bottom: 30px;
            }
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('used_cars', 'used_cars_for_sale_shortcode');

function user_car_listing_single($data)
{
    if (!$data) return '';

    $formatted_price = 'RM ' . number_format($data['price'], 0, '.', ',');
    ob_start();
?>
    <div class="car-listing-card">
        <div class="car-image-container">
            <img src="<?php echo (!empty($data['images']) ? $data['images'][0]['path']['gallery'] : 'placeholder-image.jpg'); ?>"
                alt="<?php echo esc_attr($data['title']); ?>"
                class="car-image" />
        </div>
        <div class="car-details">
            <h3 class="car-title"><?php echo esc_html($data['title']); ?></h3>
            <div class="car-price"><?php echo $formatted_price; ?></div>
        </div>
    </div>
<?php
    return ob_get_clean();
}

function get_used_cars($make, $model)
{
    $token = get_auth_token();
    $variants = get_transient('icarasia_used_cars_' . $make . '_' . $model);

    if ($variants != false) {
        return [
            'success' => true,
            'from_cache' => 'yes',
            'data' => $variants
        ];
    }

    $response = wp_remote_get('https://exapipreprod.carlist.my/v2.0/wapcar/en/listing?make=' . $make . '&model=' . $model, array(
        'method' => 'GET',
        'headers' => array(
            'token' => $token,
            'Content-Type' => 'application/json',
        ),
    ));

    if (is_wp_error($response)) {
        return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    }

    $response = json_decode(wp_remote_retrieve_body($response), true);
    $variants = $response['result'];

    set_transient('icarasia_used_cars_' . $make . '_' . $model, $variants, 60 * 60);

    return [
        'success' => true,
        'from_cache' => 'no',
        'data' => $variants
    ];
}

function get_auth_token()
{
    // Fetch the token
    $all_cars_api_auth_token_cache_key = 'wapcar_all_cars_api_auth_token';
    $token_data = get_transient($all_cars_api_auth_token_cache_key);
    if (!$token_data) {
        $response = wp_remote_post('https://exapipreprod.carlist.my/v3.0/my/en/authentication/token', array(
            'method' => 'POST',
            'headers' => array(
                'Content-Type' => 'application/json',
                'user_key' => WAPCAR_USER_KEY,
                'app_key' => WAPCAR_APP_KEY,
                'platform' => WAPCAR_PLATFORM,
                'user_secret' => WAPCAR_USER_SECRET
            )
        ));

        if (is_wp_error($response)) {
            return 'Error fetching token';
        }

        set_transient($all_cars_api_auth_token_cache_key, json_decode(wp_remote_retrieve_body($response), true), 60 * 60);

        $token_data = json_decode(wp_remote_retrieve_body($response), true);
    }

    $token = $token_data['token'];

    return $token;
}
