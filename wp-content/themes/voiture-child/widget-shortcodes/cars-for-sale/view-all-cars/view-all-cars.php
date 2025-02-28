<?php

function used_cars_shortcode($atts, $content = null)
{
    wp_enqueue_style('font-awesome', 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.7.1/css/all.min.css');

    ob_start();
?>
    <div class="car-listing-header-container">
        <h1 class="car-listing-header">
            <span class="car-listing-title">Used/Second hand, recon & new Cars for sale |</span>
            <span class="car-listing-count"> 10,412 </span>
            <span>vehicles matched</span>
        </h1>
        <p class="car-listing-description">Used/Second hand, recon & new Cars for sale in Malaysia. Compare car prices & get the best deals from our trusted dealers.</p>
        <!-- <div class="filters">
            <button class="qualified-btn">Qualified</button>
            <button class="carsome-certified-btn">CARSOME Certified</button>
        </div> -->

        <div class="car-listing-options">
            <div class="hot-deals-btn">
                <i class="fire-icon"></i>
                <i class="fa-solid fa-fire"></i>
                <span>Hot deals</span>
                <span>Daily Discount Deals</span>
            </div>
            <div class="registration-card-btn">
                <i class="registration-card-icon"></i>
                <span>Registration Card</span>
                <span>Car with a Registration Card</span>
            </div>
            <div class="video-listing-btn">

                <span style="display: flex;"> <i class="video-icon">Video</i></span>Video Listing <span></span>
                <span>Seller's Video Reviews</span>
            </div>
        </div>
    </div>

    <div class="car-listing-container">
        <div>Loading...</div>
    </div>

    <style>
        .car-listing-header-container {
            font-family: Arial, sans-serif;
            padding: 20px;
            background-color: #f5f5f5;
        }

        .car-listing-header {
            display: flex;
            flex-direction: row;
        }

        .car-listing-header {
            font-size: 20px;
            font-weight: bold;
            margin-bottom: 10px;
        }

        p {
            font-size: 12px;
            font-weight: 300;
            color: #666;
            margin-bottom: 20px;
        }

        .filters {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .qualified-btn,
        .carsome-certified-btn {
            padding: 8px 16px;
            border: 1px solid #ccc;
            border-radius: 4px;
            background-color: #fff;
            cursor: pointer;
        }

        .qualified-btn.active,
        .carsome-certified-btn.active {
            background-color: #007bff;
            color: #fff;
        }

        .view-options button {
            padding: 8px;
            border: 1px solid #ccc;
            border-radius: 4px;
            background-color: #fff;
            cursor: pointer;
        }

        .view-options button.active {
            background-color: #007bff;
            color: #fff;
        }

        .best-match-dropdown {
            padding: 8px 16px;
            border: 1px solid #ccc;
            border-radius: 4px;
            background-color: #fff;
            cursor: pointer;
        }

        .car-listing-options {
            display: flex;
            justify-content: space-between;
            margin-top: 20px;
        }

        .car-listing-options>div {
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 10px;
            border: 1px solid #ccc;
            border-radius: 4px;
            background-color: #fff;
            cursor: pointer;
            flex: 1;
            margin-right: 10px;
        }

        .car-listing-options>div:last-child {
            margin-right: 0;
        }

        .car-listing-options>div i {
            margin-right: 10px;
        }

        .car-listing-options>div span:last-child {
            color: #666;
            font-size: 14px;
        }

        .car-listing-options>div i {
            margin-right: 10px;
            font-size: 18px;
            /* Adjust the font size as needed */
        }

        .hot-deals-btn i::before {
            content: "\f06d";
            /* Fire icon */
        }

        .registration-card-btn i::before {
            content: "\f02b";
            /* Registration card icon */
        }

        .video-listing-btn i::before {
            content: "\f03d";
            /* Video icon */
        }
    </style>

<?php
    return ob_get_clean();
}
add_shortcode('used_cars', 'used_cars_shortcode');

function user_car_listing_single($data)
{
    wp_enqueue_style('view-all-cars-css', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/view-all-cars/view-all-cars.css');

    if (!$data) return '';

    $monthly_payment = calculate_monthly_payment($data['price']);
    $formatted_price = number_format($data['price'], 0, '.', ',');
    $currency_unit = $data['currency_unit'];

?>
    <div class="car-listing-card">
        <div class="car-image-container">
            <img src="<?php echo (!empty($data['images']) ? $data['images'][0] : 'placeholder-image.jpg'); ?>"
                alt="<?php echo esc_attr($data['title']); ?>"
                class="car-image" />

            <div class="price-overlay">
                <div class="monthly-price">RM <?php echo $monthly_payment; ?></div>
                <div class="total-price"><?php echo $formatted_price; ?></div>
            </div>

            <?php echo ($data['featured'] ? '<span class="featured-badge">FEATURED</span>' : ''); ?>
        </div>

        <div class="car-details">
            <h3 class="car-title"><?php echo esc_html($data['title']); ?></h3>

            <div class="details-list">
                <div class="detail-item">
                    <span class="icon">🚗</span>
                    <?php echo $data['mileage_range']; ?> KM - <?php echo $data['transmission']; ?>
                </div>

                <div class="detail-item">
                    <span class="icon">🏢</span>
                    <?php echo $data['type']; ?> Car
                </div>

                <div class="detail-item">
                    <span class="icon">📍</span>
                    <?php echo $data['area']; ?>, <?php echo $data['state']; ?>
                </div>
            </div>

            <div class="action-buttons">
                <a class="call-button">
                    Call now
                </a>
                <a class="whatsapp-button">
                    WhatsApp
                </a>
            </div>
        </div>
    </div>

<?php
    $html = ob_get_clean();

    return $html;
}


function calculate_monthly_payment($price, $years = 9, $interest_rate = 0.0265)
{
    $monthly_rate = $interest_rate / 12;
    $months = $years * 12;
    $monthly_payment = ($price * $monthly_rate * pow(1 + $monthly_rate, $months)) / (pow(1 + $monthly_rate, $months) - 1);
    return ceil($monthly_payment);
}


function car_filter_widget($atts = [])
{
    wp_enqueue_style('car-filter-widget-css', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/view-all-cars/view-all-cars.css');
    wp_enqueue_script('car-filter-widget-js', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/view-all-cars/view-all-cars.js', array('jquery'), null, true);

    // api data
    $api_data = get_api_data();
    if ($api_data['success']) {
        $api_data = $api_data['data'];
        $facets = $api_data['facets'];
    } else {
        return '';
    }

    // Get current URL path and query parameters
    $current_url = $_SERVER['REQUEST_URI'];
    $url_parts = parse_url($current_url);
    $path = $url_parts['path'];
    $query = isset($url_parts['query']) ? $url_parts['query'] : '';
    parse_str($query, $query_params);

    // Determine selected condition based on URL
    $selected_condition = '';
    if (strpos($path, 'used-cars-for-sale') !== false) {
        $selected_condition = 'used';
    } elseif (strpos($path, 'new-cars-for-sale') !== false) {
        $selected_condition = 'new';
    } elseif (strpos($path, 'recon-cars-for-sale') !== false) {
        $selected_condition = 'recon';
    } elseif (isset($query_params['type'])) {
        if ($query_params['type'] === 'certifiedpreowned') {
            $selected_condition = 'certified-pre-owned';
        } elseif ($query_params['type'] === 'carsomecertified') {
            $selected_condition = 'carsome-certified';
        }
    }
    $facet_conditions = array_key_exists('type', $facets) ? $facets['type'] : array();

    // price
    $selected_price_min = isset($query_params['price_min']) ? $query_params['price_min'] : '';
    $selected_price_max = isset($query_params['price_max']) ? $query_params['price_max'] : '';

    // year
    $selected_year_min = isset($query_params['year_min']) ? $query_params['year_min'] : '';
    $selected_year_max = isset($query_params['year_max']) ? $query_params['year_max'] : '';
    $current_year = intval(date('Y'));

    // mileage
    $selected_mileage_min = isset($query_params['mileage_min']) ? $query_params['mileage_min'] : '';
    $selected_mileage_max = isset($query_params['mileage_max']) ? $query_params['mileage_max'] : '';

    // body type
    $selected_body_types = isset($query_params['body_type']) ? explode(',', $query_params['body_type']) : [];
    $facet_body_types = array_key_exists('body', $facets) ? $facets['body'] : array();

    // color
    $selected_colors = isset($query_params['color']) ? explode(',', $query_params['color']) : [];
    $facet_colors = array_key_exists('color', $facets) ? $facets['color'] : array();

    // transmission
    $selected_transmission = isset($query_params['transmission']) ? $query_params['transmission'] : '';
    $facet_transmissions = array_key_exists('transmission', $facets) ? $facets['transmission'] : array();

    // fuel type
    $selected_fuel_type = isset($query_params['fuel_type']) ? $query_params['fuel_type'] : '';
    $facet_fuel_types = array_key_exists('fuel_type', $facets) ? $facets['fuel_type'] : array();

    // driven wheel
    $selected_driven_wheel = isset($query_params['driven_wheel']) ? $query_params['driven_wheel'] : '';
    $facet_driven_wheels = array_key_exists('driven_wheel', $facets) ? $facets['driven_wheel'] : array();

    // seller type
    $selected_seller_type = isset($query_params['seller_type']) ? $query_params['seller_type'] : '';
    $facet_seller_types = array_key_exists('profile_type', $facets) ? $facets['profile_type'] : array();

    ob_start();
?>
    <div class="car-filter-container">
        <div class="filter-header">
            <h3>Search Filters</h3>
            <a href="/cars-for-sale/malaysia" class="clear-all">Clear All</a>
        </div>

        <!-- Condition Filter -->
        <?php if (!empty($facet_conditions)) {
        ?>
            <div class="filter-section">
                <div class="filter-header-collapsible">
                    <h4>Condition</h4>
                    <button class="toggle-btn">
                        <span class="arrow">▼</span>
                    </button>
                </div>
                <div class="filter-content">
                    <div class="checkbox-group">
                        <?php
                        foreach ($facet_conditions as $condition => $count) : ?>
                            <label class="checkbox-container">
                                <input type="checkbox" name="condition" value="<?php echo $condition; ?>"
                                    <?php checked($selected_condition === $condition); ?>>
                                <span class="checkmark"></span>
                                <?php echo $condition; ?>
                            </label>
                        <?php endforeach; ?>

                        <label class="checkbox-container">
                            <input type="checkbox" name="condition" value="certified-pre-owned"
                                <?php checked($selected_condition === 'certified-pre-owned'); ?>
                                data-url="/cars-for-sale/malaysia?type=certifiedpreowned">
                            <span class="checkmark"></span>
                            Certified Pre-Owned Cars
                        </label>
                        <label class="checkbox-container">
                            <input type="checkbox" name="condition" value="carsome-certified"
                                <?php checked($selected_condition === 'carsome-certified'); ?>
                                data-url="/cars-for-sale/malaysia?type=carsomecertified">
                            <span class="checkmark"></span>
                            Carsome Certified Cars
                        </label>
                    </div>
                </div>
            </div>

        <?php }
        ?>

        <!-- transmission filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Transmission</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="checkbox-group">
                    <?php foreach ($facet_transmissions as $transmission => $count) : ?>
                        <label class="checkbox-container">
                            <input type="checkbox" name="transmission" value="<?php echo $transmission; ?>"
                                <?php checked($selected_transmission === $transmission); ?>>
                            <span class="checkmark"></span>
                            <?php echo $transmission; ?>
                        </label>
                    <?php endforeach ?>
                </div>
            </div>
        </div>

        <!-- fuel type filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Fuel Type</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="checkbox-group">
                    <?php foreach ($facet_fuel_types as $fuel_type => $count) : ?>
                        <label class="checkbox-container">
                            <input type="checkbox" name="fuel_type" value="<?php echo $fuel_type; ?>"
                                <?php checked($selected_fuel_type === $fuel_type); ?>>
                            <span class="checkmark"></span>
                            <?php echo $fuel_type; ?>
                        </label>
                    <?php endforeach ?>
                </div>
            </div>
        </div>

        <!-- driven wheel filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Driven Wheel</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="checkbox-group">
                    <?php foreach ($facet_driven_wheels as $driven_wheel => $count) : ?>
                        <label class="checkbox-container">
                            <input type="checkbox" name="driven_wheel" value="<?php echo $driven_wheel; ?>"
                                <?php checked($selected_driven_wheel === $driven_wheel); ?>>
                            <span class="checkmark"></span>
                            <?php echo $driven_wheel; ?>
                        </label>
                    <?php endforeach ?>
                </div>
            </div>
        </div>

        <!-- seller type filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Seller Type</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="checkbox-group">
                    <?php foreach ($facet_seller_types as $seller_type => $count) : ?>
                        <?php if ($seller_type !== ''): ?>
                            <label class="checkbox-container">
                                <input type="checkbox" name="seller_type" value="<?php echo $seller_type; ?>"
                                    <?php checked($selected_seller_type === $seller_type); ?>>
                                <span class="checkmark"></span>
                                <?php echo $seller_type; ?>
                            </label>
                        <?php endif; ?>
                    <?php endforeach ?>
                </div>
            </div>
        </div>

        <!-- Price Filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Price</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="price-filter-container">
                    <label for="price-min">Min:</label>
                    <select name="price-min" id="price-min">
                        <option value="">Any</option>
                        <option value="10000" <?php selected($selected_price_min, '10000'); ?>>RM 10,000</option>
                        <option value="20000" <?php selected($selected_price_min, '20000'); ?>>RM 20,000</option>
                        <option value="30000" <?php selected($selected_price_min, '30000'); ?>>RM 30,000</option>
                        <option value="40000" <?php selected($selected_price_min, '40000'); ?>>RM 40,000</option>
                        <option value="50000" <?php selected($selected_price_min, '50000'); ?>>RM 50,000</option>
                        <option value="60000" <?php selected($selected_price_min, '60000'); ?>>RM 60,000</option>
                        <option value="70000" <?php selected($selected_price_min, '70000'); ?>>RM 70,000</option>
                        <option value="80000" <?php selected($selected_price_min, '80000'); ?>>RM 80,000</option>
                        <option value="90000" <?php selected($selected_price_min, '90000'); ?>>RM 90,000</option>
                    </select>

                    <label for="price-max">Max:</label>
                    <select name="price-max" id="price-max">
                        <option value="">Any</option>
                        <option value="10000" <?php selected($selected_price_max, '10000'); ?>>RM 10,000</option>
                        <option value="20000" <?php selected($selected_price_max, '20000'); ?>>RM 20,000</option>
                        <option value="30000" <?php selected($selected_price_max, '30000'); ?>>RM 30,000</option>
                        <option value="40000" <?php selected($selected_price_max, '40000'); ?>>RM 40,000</option>
                        <option value="50000" <?php selected($selected_price_max, '50000'); ?>>RM 50,000</option>
                        <option value="60000" <?php selected($selected_price_max, '60000'); ?>>RM 60,000</option>
                        <option value="70000" <?php selected($selected_price_max, '70000'); ?>>RM 70,000</option>
                        <option value="80000" <?php selected($selected_price_max, '80000'); ?>>RM 80,000</option>
                        <option value="90000" <?php selected($selected_price_max, '90000'); ?>>RM 90,000</option>
                    </select>
                </div>
            </div>
        </div>

        <!-- year filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Year</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="year-filter-container">
                    <label for="year-min">Min:</label>
                    <select name="year-min" id="year-min">
                        <option value="">Any</option>
                        <?php
                        $current_year = date('Y');

                        for ($i = $current_year - 5; $i <= $current_year; $i++) {
                            echo '<option value="' . $i . '" ' . selected($selected_year_min, $i) . '>' . $i . '</option>';
                        }
                        ?>
                    </select>

                    <label for="year-max">Max:</label>
                    <select name="year-max" id="year-max">
                        <option value="">Any</option>
                        <?php
                        for ($i = $current_year - 5; $i <= $current_year; $i++) {
                            echo '<option value="' . $i . '" ' . selected($selected_year_max, $i) . '>' . $i . '</option>';
                        }
                        ?>
                    </select>
                </div>
            </div>
        </div>

        <!-- mileage filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Mileage</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="mileage-filter-container">
                    <label for="mileage-min">Min:</label>
                    <select name="mileage-min" id="mileage-min">
                        <option value="">Any</option>
                        <?php
                        for ($i = 0; $i <= 100000; $i += 20000) {
                            echo '<option value="' . $i . '" ' . selected($selected_mileage_min, $i) . '>' . $i . '</option>';
                        }
                        ?>
                    </select>

                    <label for="mileage-max">Max:</label>
                    <select name="mileage-max" id="mileage-max">
                        <option value="">Any</option>
                        <?php
                        for ($i = 0; $i <= 100000; $i += 20000) {
                            echo '<option value="' . $i . '" ' . selected($selected_mileage_max, $i) . '>' . $i . '</option>';
                        }
                        ?>
                    </select>
                </div>
            </div>
        </div>

        <!-- body type filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Body Type</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="body-type-filter-container">
                    <?php foreach ($facet_body_types as $body_type => $count) : ?>
                        <label class="checkbox-container">
                            <input type="checkbox" name="body_type" value="<?php echo $body_type; ?>"
                                <?php checked(in_array($body_type, $selected_body_types)); ?>>
                            <span class="checkmark"></span>
                            <?php echo $body_type; ?> (<?php echo $count; ?>)
                        </label>
                    <?php endforeach; ?>
                </div>
            </div>
        </div>

        <!-- Color Filter -->
        <div class="filter-section">
            <div class="filter-header-collapsible">
                <h4>Color</h4>
                <button class="toggle-btn">
                    <span class="arrow">▼</span>
                </button>
            </div>
            <div class="filter-content">
                <div class="color-filter-container">
                    <?php foreach ($facet_colors as $color => $count) : ?>
                        <label class="checkbox-container">
                            <input type="checkbox" name="color" value="<?php echo $color; ?>"
                                <?php checked(in_array($color, $selected_colors)); ?>>
                            <span class="checkmark"></span>
                            <?php echo $color; ?> (<?php echo $count; ?>)
                        </label>
                    <?php endforeach; ?>
                </div>
            </div>
        </div>

    </div>

    <style>
        .car-filter-container {
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }

        .filter-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .filter-header-collapsible {
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }

        .filter-header-collapsible h4 {
            margin: 0;
            color: #333;
        }

        .toggle-btn {
            background: none;
            border: none;
            cursor: pointer;
            padding: 0;
            color: #666;
        }

        .arrow {
            display: inline-block;
            transition: transform 0.3s ease;
        }

        .collapsed .arrow {
            transform: rotate(-90deg);
        }

        .filter-content {
            padding: 10px 0;
            display: block;
            transition: max-height 0.3s ease-out;
            overflow: hidden;
        }

        .collapsed .filter-content {
            display: none;
        }

        .checkbox-container {
            display: block;
            position: relative;
            padding-left: 35px;
            margin-bottom: 12px;
            cursor: pointer;
            font-size: 14px;
        }

        .checkbox-container input {
            position: absolute;
            opacity: 0;
            cursor: pointer;
        }

        .checkmark {
            position: absolute;
            left: 0;
            top: 0;
            height: 20px;
            width: 20px;
            background-color: #fff;
            border: 2px solid #ddd;
            border-radius: 4px;
        }

        .checkbox-container:hover input~.checkmark {
            background-color: #f5f5f5;
        }

        .checkbox-container input:checked~.checkmark {
            background-color: #0066cc;
            border-color: #0066cc;
        }

        .checkbox-container input:checked~.checkmark:after {
            content: "";
            position: absolute;
            display: block;
            left: 6px;
            top: 2px;
            width: 5px;
            height: 10px;
            border: solid white;
            border-width: 0 2px 2px 0;
            transform: rotate(45deg);
        }

        .clear-all {
            color: #0066cc;
            text-decoration: none;
            font-size: 14px;
        }


        /* price, year, mileage css */
        .price-filter-container,
        .year-filter-container,
        .mileage-filter-container {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .price-filter-container label,
        .year-filter-container label,
        .mileage-filter-container label {
            font-size: 14px;
            color: #333;
        }

        .price-filter-container select,
        .year-filter-container select,
        .mileage-filter-container select {
            flex: 1;
            padding: 8px 12px;
            font-size: 14px;
            border: 1px solid #ccc;
            border-radius: 4px;
            background-color: #fff;
            appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M7 10l5 5 5-5z'/%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 8px center;
            background-size: 20px;
            cursor: pointer;
        }

        .price-filter-container select:focus,
        .year-filter-container select:focus,
        .mileage-filter-container select:focus {
            outline: none;
            border-color: #0066cc;
        }
    </style>

    <script>
        jQuery(document).ready(function($) {
            selectedfilters = {
                condition: [],
                price_min: '',
                price_max: '',
                year_min: '',
                year_max: '',
                mileage_min: '',
                mileage_max: '',
                transmission: [],
                body_type: [],
                color: [],
                fuel_type: [],
                seller_type: []
            };

            // Toggle filter sections
            $('.filter-header-collapsible').click(function() {
                $(this).parent().toggleClass('collapsed');
                $(this).find('.arrow').css('transform',
                    $(this).parent().hasClass('collapsed') ? 'rotate(-90deg)' : 'rotate(0deg)');
            });

            // Initialize all sections as collapsed
            $('.filter-section').addClass('collapsed');

            // render cars
            updateCarListings();

            // Handle condition checkbox changes
            $('input[name="condition"]').on('change', function() {
                // Uncheck other checkboxes
                $('input[name="condition"]').not(this).prop('checked', false);

                if ($(this).is(':checked')) {
                    // history.pushState(null, null, $(this).data('url'));
                    selectedfilters.condition = [$(this).val()];
                    console.log(selectedfilters);
                } else {
                    // If no checkbox is checked, go to default URL
                    // history.pushState(null, null, '/cars-for-sale/malaysia/');
                    selectedfilters.condition = [];
                    console.log(selectedfilters);
                }

                updateCarListings();
            });

            // Handle price range changes
            $('#price-min, #price-max').on('change', function() {
                selectedfilters.price_min = $('#price-min').val();
                selectedfilters.price_max = $('#price-max').val();
                updateCarListings();
            });

            // Handle year range changes
            $('#year-min, #year-max').on('change', function() {
                selectedfilters.year_min = $('#year-min').val();
                selectedfilters.year_max = $('#year-max').val();
                updateCarListings();
            })

            // Handle mileage range changes
            $('#mileage-min, #mileage-max').on('change', function() {
                selectedfilters.mileage_min = $('#mileage-min').val();
                selectedfilters.mileage_max = $('#mileage-max').val();
                updateCarListings();
            })

            // Handle Transmission checkbox changes
            $('input[name="transmission"]').on('change', function() {
                // Uncheck other checkboxes
                $('input[name="transmission"]').not(this).prop('checked', false);

                if ($(this).is(':checked')) {
                    // history.pushState(null, null, $(this).data('url'));
                    selectedfilters.transmission = [$(this).val()];
                    console.log(selectedfilters);
                } else {
                    // If no checkbox is checked, go to default URL
                    // history.pushState(null, null, '/cars-for-sale/malaysia/');
                    selectedfilters.transmission = [];
                    console.log(selectedfilters);
                }

                updateCarListings();
            })

            // Handle body type checkbox changes
            $('input[name="body_type"]').on('change', function() {
                // Uncheck other checkboxes
                $('input[name="body_type"]').not(this).prop('checked', false);

                if ($(this).is(':checked')) {
                    // history.pushState(null, null, $(this).data('url'));
                    selectedfilters.body_type = [$(this).val()];
                    console.log(selectedfilters);
                } else {
                    // If no checkbox is checked, go to default URL
                    // history.pushState(null, null, '/cars-for-sale/malaysia/');
                    selectedfilters.body_type = [];
                    console.log(selectedfilters);
                }

                updateCarListings();
            })

            // Handle color checkbox changes
            $('input[name="color"]').on('change', function() {
                // Uncheck other checkboxes
                $('input[name="color"]').not(this).prop('checked', false);

                if ($(this).is(':checked')) {
                    // history.pushState(null, null, $(this).data('url'));
                    selectedfilters.color = [$(this).val()];
                    console.log(selectedfilters);
                } else {
                    // If no checkbox is checked, go to default URL
                    // history.pushState(null, null, '/cars-for-sale/malaysia/');
                    selectedfilters.color = [];
                    console.log(selectedfilters);
                }

                updateCarListings();
            })

            // Handle Fuel Type checkbox changes
            $('input[name="fuel_type"]').on('change', function() {
                // Uncheck other checkboxes
                $('input[name="fuel_type"]').not(this).prop('checked', false);

                if ($(this).is(':checked')) {
                    // history.pushState(null, null, $(this).data('url'));
                    selectedfilters.fuel_type = [$(this).val()];
                    console.log(selectedfilters);
                } else {
                    // If no checkbox is checked, go to default URL
                    // history.pushState(null, null, '/cars-for-sale/malaysia/');
                    selectedfilters.fuel_type = [];
                    console.log(selectedfilters);
                }

                updateCarListings();
            })

            // Handle Seller Type checkbox changes
            $('input[name="seller_type"]').on('change', function() {
                // Uncheck other checkboxes
                $('input[name="seller_type"]').not(this).prop('checked', false);

                if ($(this).is(':checked')) {
                    // history.pushState(null, null, $(this).data('url'));
                    selectedfilters.seller_type = [$(this).val()];
                    console.log(selectedfilters);
                } else {
                    // If no checkbox is checked, go to default URL
                    // history.pushState(null, null, '/cars-for-sale/malaysia/');
                    selectedfilters.seller_type = [];
                    console.log(selectedfilters);
                }

                updateCarListings();
            })

            function updateCarListings() {
                // change car listing header content based on selected fil

                // make an ajax call to update content in class car-listing-container
                jQuery.ajax({
                    url: '/wp-admin/admin-ajax.php',
                    type: 'POST',
                    data: {
                        action: 'update_car_listings',
                        selectedfilters: selectedfilters
                    },
                    success: function(response) {
                        // update count of car listing
                        console.log($('.car-listing-count'));
                        $('.car-listing-count').text(response.count);
                        $('.car-listing-container').html(response);
                    },
                    error: function(xhr, status, error) {
                        console.error('Error:', error);
                    }
                });
            }
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('car_filter', 'car_filter_widget');

function update_car_listings_handler()
{
    $selectedfilters = $_POST['selectedfilters'];
    $filtered_cars = get_filtered_cars($selectedfilters);

    ob_start();
    foreach ($filtered_cars as $car) {
        echo user_car_listing_single($car);
    }

    $data = ob_get_clean();

    wp_send_json_success(['message' => 'Success', 'count' => count($filtered_cars), 'data' => $data]);
}
add_action('wp_ajax_update_car_listings', 'update_car_listings_handler');
add_action('wp_ajax_nopriv_update_car_listings', 'update_car_listings_handler');


function get_filtered_cars($filters)
{
    // $all_cars = get_all_cars();
    $all_cars = get_api_data()['data']['result'];
    // splice first 10 items from the array
    $all_cars = array_slice($all_cars, 0, 10);
    foreach ($filters as $key => $value) {
        switch ($key) {
            case 'condition':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if (strtolower($car['type']) == strtolower($value[0])) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'price_min':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['price'] >= $value) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'price_max':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['price'] <= $value) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'year_min':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['year'] >= $value) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'year_max':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['year'] <= $value) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'mileage_min':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['mileage_range'] >= $value) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'mileage_max':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['mileage_range'] <= $value) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'body_type':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['body_type'] == $value[0]) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'color':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['color'] == $value[0]) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'transmission':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['transmission'] == $value[0]) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'fuel_type':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if (strtolower($car['fuel_type']) == $value[0]) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'driven_wheel':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['driven_wheel'] == $value[0]) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            case 'seller_type':
                if (!empty($value)) {
                    $filtered_cars = [];
                    foreach ($all_cars as $index => $car) {
                        if ($car['seller_type'] == $value[0]) {
                            $filtered_cars[] = $car;
                        }
                    }
                    $all_cars = $filtered_cars;
                }
                break;

            default:
                break;
        }
    }

    return $all_cars;
}


// remember to remove this function after API is ready
function get_all_cars()
{
    $all_cars = [
        [
            "listing_id" => 13492201,
            "title" => "2014 Toyota Camry 2.0 G X Sedan",
            "price" => 59888,
            "mileage_range" => "40,000 - 87500",
            "transmission" => "Automatic",
            "type" => "Used",
            "area" => "Klang",
            "state" => "Selangor",
            "mobile_phone" => "+60126162996",
            "featured" => true,
            'fuel_type' => 'petrol',
            'driven_wheel' => 'RWD (Rear Wheel Drive)'
        ],
        [
            "listing_id" => 13492201,
            "title" => "2014 Toyota Camry 2.0 G X Sedan2",
            "price" => 59888,
            "mileage_range" => "40,000 - 87500",
            "transmission" => "Automatic",
            "type" => "New",
            "area" => "Klang",
            "state" => "Selangor",
            "mobile_phone" => "+60126162996",
            "featured" => true,
            'fuel_type' => 'diesel',
            'driven_wheel' => 'FWD (Front Wheel Drive)'
        ],
    ];

    return $all_cars;
}

function get_api_data()
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

    // Fetch the actual data
    $all_listings_api_data_cache_key = 'wapcar_all_listings_api_data';
    $api_data = get_transient($all_listings_api_data_cache_key);
    if (!$api_data) {
        return ['success' => true, 'from_cache' => 'yes', 'data' => $api_data];
    }

    $data_response = wp_remote_get('https://exapipreprod.carlist.my/v2.0/wapcar/en/listing', array(
        'headers' => array(
            'Content-Type' => 'application/json',
            'token' => $token
        )
    ));

    if (is_wp_error($data_response)) {
        return ['success' => false, 'message' => 'Error fetching data'];
    }

    $api_data = json_decode(wp_remote_retrieve_body($data_response), true);

    set_transient($all_listings_api_data_cache_key, $api_data, 3600);

    return ['success' => true, 'from_cache' => 'no', 'data' => $api_data];
}
