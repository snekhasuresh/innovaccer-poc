<?php

add_shortcode('single_variant_specs', 'single_variant_specs_shortcode');
function single_variant_specs_shortcode()
{
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $variant_section = get_query_var('variant_section');
    $listing_name = $make . '-' . $listing_name;

    // get listing post by post name
    $listing_post_query = new WP_Query(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));

    // if no listing post found, return
    if (!$listing_post_query->have_posts()) {
        return;
    }
    $listing_post = $listing_post_query->post;
    $listing_meta = get_post_meta($listing_post->ID);

    // get all variants of the listing using post parent
    $args = array(
        'post_type' => 'variant',
        'posts_per_page' => 1,
        'post_parent' => $listing_post->ID,
        'name' => $variant_section
    );
    $variants = new WP_Query($args);

    // if no variants found, return
    if (!$variants->have_posts()) {
        return;
        wp_die('No Variants Found');
    }

    $variant_posts = $variants->posts;

    $variant_spec_groups = [
        'Price' => [['label' => 'Retail Price', 'key' => 'retail_price']],
        'Costs' => [
            ['label' => 'Insurance', 'key' => 'insurance'],
            ['label' => 'Road Tax', 'key' => 'road_tax'],
            ['label' => 'Monthly Payment', 'key' => 'monthly_payment']
        ],
        'Overview' => [
            ['label' => 'Make', 'key' => 'make'],
            ['label' => 'Body Type', 'key' => 'body_type'],
            ['label' => 'Segment', 'key' => 'segment'],
            ['label' => 'Fuel Type', 'key' => 'fuel_type'],
            ['label' => 'Model', 'key' => 'model'],
            ['label' => 'Launched Year', 'key' => 'launched_year'],
            ['label' => 'Horsepower (ps)', 'key' => 'horsepower'],
            ['label' => 'Torque (Nm)', 'key' => 'torque'],
            ['label' => 'Engine', 'key' => 'engine'],
            ['label' => 'Engine Power(PS)', 'key' => 'engine_power'],
            ['label' => 'Electric Engine(PS)', 'key' => 'electric_engine'],
            ['label' => 'Transmission', 'key' => 'transmission'],
            ['label' => 'Length*Width*Height(mm)', 'key' => 'dimensions'],
            ['label' => '0-100 km/h (s)', 'key' => '0-100_kmph'],
            ['label' => 'Manufacturers Claim(L/100km)', 'key' => 'manufacturers_claim'],
            ['label' => 'As Tested(L/100km)', 'key' => 'as_tested'],
            ['label' => 'On Sale', 'key' => 'on_sale'],
            ['label' => 'Manufacturer Warranty', 'key' => 'manufacturer_warranty'],
            ['label' => 'Top Speed (km/h)', 'key' => 'top_speed'],
        ],
        'Dimensions' => [
            ['label' => 'Length(mm)', 'key' => 'length'],
            ['label' => 'Width(mm)', 'key' => 'width'],
            ['label' => 'Height(mm)', 'key' => 'height'],
            ['label' => 'Wheelbase(mm)', 'key' => 'width_copy'], //need to change name
            ['label' => 'Weight(kg)', 'key' => 'weight'],
            ['label' => 'Ground Clearance', 'key' => 'ground_clearance'],
            ['label' => 'Doors', 'key' => 'doors'], //problem
            ['label' => 'Seats', 'key' => 'seats'],
            ['label' => 'Fuel Tank(litres)', 'key' => 'fuel_tank'], //problem
            ['label' => 'Boot Space(L)', 'key' => 'boot_space'],
            ['label' => 'Powertrain', 'key' => 'powertrain'], //not there
            ['label' => 'Capacity (cc)', 'key' => 'capacity'], //not there
            ['label' => 'Capacity (L)', 'key' => 'capacity_l'], //not there
            ['label' => 'Aspiration Form', 'key' => 'aspiration_form'],
            ['label' => 'Cylinder Arrangement', 'key' => 'cylinder_arrangement'],
            ['label' => 'Number of Cylinders', 'key' => 'number_of_cylinders'],
            ['label' => 'Engine Power(PS)', 'key' => 'engine_power'],
            ['label' => 'Engine Power(kW)', 'key' => 'engine_power_kW'], //not there
            ['label' => 'Rpm at Max Hp(RPM)', 'key' => 'rpm_at_max_hp'],
            ['label' => 'Engine Torque(Nm)', 'key' => 'engine_torque'],
            ['label' => 'Rpm at Max torque(RPM)', 'key' => 'rpm_at_max_torque'],
        ],
        'Powertrain' => [
            ['label' => 'Capacity (cc)', 'key' => 'capacity'],
            ['label' => 'Capacity (L)', 'key' => 'capacity_l'], //not there
            ['label' => 'Aspiration Form', 'key' => 'aspiration_form'],
            ['label' => 'Cylinder Arrangement', 'key' => 'cylinder_arrangement'],
            ['label' => 'Number of Cylinders', 'key' => 'number_of_cylinders'],
            ['label' => 'Engine Power(PS)', 'key' => 'engine_power'],
            ['label' => 'Engine Power(kW)', 'key' => 'engine_power_kW'], //not there
            ['label' => 'Rpm at Max Hp(RPM)', 'key' => 'rpm_at_max_hp'],
            ['label' => 'Engine Torque(Nm)', 'key' => 'engine_torque'],
            ['label' => 'Rpm at Max torque(RPM)', 'key' => 'rpm_at_max_torque'],
        ],
        'Electric Motor' => [
            ['label' => 'Motor Type', 'key' => 'motor_type'],
            ['label' => 'Motor Output(PS)', 'key' => 'motor_output'],
            ['label' => 'Motor Torque(Nm)', 'key' => 'motor_torque'],
            ['label' => 'Front Motor Output(kW)', 'key' => 'front_motor_output'],
            ['label' => 'Front Motor Torque(Nm)', 'key' => 'front_motor_torque'],
            ['label' => 'Rear Motor Output(kW)', 'key' => 'rear_motor_output'],
            ['label' => 'Rear Motor Torque(Nm)', 'key' => 'rear_motor_torque'],
            ['label' => 'Combined System Output(PS)', 'key' => 'combined_system_output'],
            ['label' => 'Combined System Torque(Nm)', 'key' => 'combined_system_torque'],
            ['label' => 'Number Of Motors', 'key' => 'number_of_motors'],
            ['label' => 'Motor Arrangement', 'key' => 'motor_arrangement'],
            ['label' => 'Battery Type', 'key' => 'battery_type'],
            ['label' => 'EV Range(km)', 'key' => 'ev_range'],
            ['label' => 'Battery Capacity(kWh)', 'key' => 'battery_capacity'],
            ['label' => 'Power Consumption Per 100 Kilometers (kWh)', 'key' => 'power_consumption_per_100km'],
            ['label' => 'Battery Warranty', 'key' => 'battery_warranty'],
            ['label' => 'Quick Charge Time(h)', 'key' => 'quick_charge_time'],
            ['label' => 'Slow Charge Time(h)', 'key' => 'slow_charge_time'],
        ],
        'Drive Train' => [
            ['label' => 'Transmission', 'key' => 'transmission'],
            ['label' => 'Forward Ratio', 'key' => 'forward_ratio'],
        ],
        'Chasis' => [
            ['label' => 'Driven Wheels', 'key' => 'driven_wheels'],
            ['label' => 'Front Suspension', 'key' => 'front_suspension'],
            ['label' => 'Rear Suspension', 'key' => 'rear_suspension'],
            ['label' => 'Adaptive Suspension', 'key' => 'adaptive_suspension'],
            ['label' => 'Front Tyres', 'key' => 'front_tyres'],
            ['label' => 'Rear Tyres', 'key' => 'rear_tyres'],
            ['label' => 'Spare Tyre', 'key' => 'spare_tyre'],
        ],
        'Brakes and Wheels' => [
            ['label' => 'Front Brakes', 'key' => 'front_brakes'],
            ['label' => 'Rear Brakes', 'key' => 'rear_brakes'],
            ['label' => 'Steering', 'key' => 'steering'],
            ['label' => 'Parking Brake', 'key' => 'parking_brake'],

        ],
        'Safety' => [
            ['label' => 'Euro NCAP Rating', 'key' => 'euro_ncap_rating'],
            ['label' => 'ASEAN NCAP Rating', 'key' => 'asean_ncap_rating'],
            ['label' => 'Airbags', 'key' => 'airbags'],
            // ['label' => 'Driver/Front Passenger Seat Airbags', 'key' => 'driverfront_seat_passenger_airbags'],
            ['label' => 'Front/Rear Side Airbags', 'key' => 'frontrear_side_airbags'],
            ['label' => 'Front/Rear Curtain Airbags', 'key' => 'frontrear_curtain_airbags'],
            ['label' => 'Knee Airbags', 'key' => 'knee_airbags'],
            ['label' => 'Rear Airbags', 'key' => 'rear_airbags'],
            ['label' => 'Airbag Disable Option', 'key' => 'airbag_disable_option'],
            ['label' => 'Seatbelt Reminder', 'key' => 'seat_belt_reminder'],
            // ['label' => 'Autonomous Emergency Braking', 'key' => 'autonomous_emergency_braking'],
            ['label' => 'Lane-keeping Alert', 'key' => 'lane_keeping_alert'],
            ['label' => 'Blind Spot Info System', 'key' => 'blind_spot_info_system'],
            ['label' => 'Collision Warning', 'key' => 'collision_warning'],
            ['label' => 'ABS/EBD', 'key' => 'abs_ebd'], // problem
            ['label' => 'Electronic Stability Control(ESC)', 'key' => 'electronic_stability_control'], // problem
            ['label' => 'ISOFIX', 'key' => 'isofix'],
            ['label' => 'Assist System', 'key' => 'assist_system'], // problem
            ['label' => 'Parking Sensor Front', 'key' => 'parking_sensor_front'],
            ['label' => 'Parking Sensor Rear', 'key' => 'parking_sensor_rear'],
            ['label' => 'Parking Camera', 'key' => 'parking_camera'],
            ['label' => 'Cruise Control', 'key' => 'cruise_control'],
            ['label' => 'Auto Parking', 'key' => 'auto_parking'],
            ['label' => 'Auto Start/Stop', 'key' => 'auto_start_stop'],
            ['label' => 'Hill Hold Assist', 'key' => 'hill_hold_assist'],
        ],
        'Assist System' => [
            ['label' => 'Parking Sensor Front', 'key' => 'parking_sensor_front'],
            ['label' => 'Parking Sensor Rear', 'key' => 'parking_sensor_rear'],
            ['label' => 'Parking Camera', 'key' => 'parking_camera'],
            ['label' => 'Cruise Control', 'key' => 'cruise_control'],
            ['label' => 'Auto Parking', 'key' => 'auto_parking'],
            ['label' => 'Auto Start/Stop', 'key' => 'auto_start_stop'],
            ['label' => 'Hill Hold Assist', 'key' => 'hill_hold_assist'],
        ],
        'Exterior' => [
            ['label' => 'Door Lock', 'key' => 'door_lock'],
            ['label' => 'Folding Wing Mirrors', 'key' => 'folding_wing_mirror'],
            ['label' => 'Auto Wipers', 'key' => 'auto_wipers'],
        ],
        'Lighting' => [
            ['label' => 'Head Lamps', 'key' => 'head_lamps'],
            ['label' => 'Tail Lamps', 'key' => 'tail_lamps'],
            ['label' => 'Daytime Running Lights', 'key' => 'day_time_running_lights'],
            ['label' => 'Front Fog Lamps', 'key' => 'front_fog_lamps'],
            ['label' => 'Rear Fog Lamps', 'key' => 'rear_fog_lamps_copy'], // need to change
            ['label' => 'Interior Lighting', 'key' => 'interior_lighting'],
            ['label' => 'Auto Headlamps', 'key' => 'auto_headlamps'],
        ],
        'Interior' => [
            ['label' => 'Sunroof', 'key' => 'sun_roof'],
            ['label' => 'Sunshade', 'key' => 'sun_shade'],
            ['label' => 'Seat Features(Front-Driver)', 'key' => 'seat_features_front_driver'],
            ['label' => 'Seat Features(Passenger)', 'key' => 'seat_features_passenger'],
            ['label' => 'Seat Features(Rear)', 'key' => 'seat_features_rear'],
            ['label' => 'Front Air-con', 'key' => 'front_air_con'],
            ['label' => 'Rear Air-con', 'key' => 'rear_air_con'],
            ['label' => 'Paddle Shift', 'key' => 'paddle_shift'],
            ['label' => 'Multi-function Steering Wheel', 'key' => 'multi_function_steering_wheel'],
            ['label' => 'Adjustable Steering Wheels', 'key' => 'adjustable_steering_wheels'],
            ['label' => 'Steering Adjustment', 'key' => 'steering_adjustment'],
            ['label' => 'Head-up Display', 'key' => 'head_up_display'],
        ],
        'Multimedia' => [
            ['label' => 'Instrument Cluster', 'key' => 'instrument_cluster'],
            ['label' => 'Screen', 'key' => 'screen'],
            ['label' => 'Screen Size（inch)', 'key' => 'screen_size'],
            ['label' => 'Rear Entertainment', 'key' => 'rear_entertainment'],
            ['label' => 'Power Socket', 'key' => 'power_socket'],
            ['label' => 'Speaker Brand', 'key' => 'speaker_brand'],
            ['label' => 'Sound Plus Functions', 'key' => 'sound_plus_functions'],
            ['label' => 'Speakers', 'key' => 'speakers'],
        ],
    ];

    $variant_spec_data = array();

    foreach ($variant_posts as $variant_post) {
        $variant_meta = get_post_meta($variant_post->ID);

        foreach ($variant_spec_groups as $group => $specs) {
            $variant_spec_data[$variant_post->post_title][$group] = [];

            foreach ($specs as $spec) {
                $key = $spec['key'];

                if ($group == 'Overview') {
                    switch ($spec['key']) {
                        case 'make':
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $make;
                            break;

                        case 'model':
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $listing_post->post_title;
                            break;

                        case 'segment':
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $listing_meta['listing-segment'][0];
                            break;

                        case 'body_type':
                            $body_type = $listing_meta['_listing_type'][0];
                            $body_type = get_term_by('id', $body_type, 'listing_type')->name;
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $body_type;
                            break;

                        case 'dimensions':
                            $length = isset($variant_meta['length'][0]) ? explode(" ", $variant_meta['length'][0])[0] : '-';
                            $width = isset($variant_meta['width'][0]) ?  explode(" ", $variant_meta['width'][0])[0] : '-';
                            $height = isset($variant_meta['height'][0]) ? explode(" ", $variant_meta['height'][0])[0] : '-';
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $length . ' x ' . $width . ' x ' . $height;
                            break;

                        default:
                            $value = isset($variant_meta[$key][0]) ? $variant_meta[$key][0] : '-';
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $value;
                            break;
                    }
                } else {
                    $value = isset($variant_meta[$key][0]) ? $variant_meta[$key][0] : '-';
                    $variant_spec_data[$variant_post->post_title][$group][$key] = $value;
                }
            }
        }
    }

    ob_start();
?>
    <div class="comparison-container">
        <!-- Sidebar -->
        <div class="sidebar-menu">
            <ul id="menu">
                <?php foreach ($variant_spec_groups as $group => $specs) : ?>
                    <li id="<?= strtolower($group) ?>" class="menu-item"><a href="#<?= $group ?>" id="<?= strtolower($group) ?>"><?= $group ?></a></li>
                <?php endforeach; ?>
            </ul>
        </div>

        <!-- Main Content -->
        <div class="parent-comparision">
            <div class="comparison-content">
                <div id="price" class="sectioning">
                    <table class="data_table">

                        <thead>
                            <tr class="header-col">
                                <td class="header-col-span">
                                    <div class="toggle-switch">
                                        <label class="switch">
                                            <input type="checkbox">
                                            <span class="specs-slider"></span>
                                        </label>
                                        <span>Hide The Same</span>
                                    </div>
                                </td>
                            </tr>
                        </thead>

                        <tbody>
                            <?php foreach ($variant_spec_groups as $group => $specs) : ?>
                                <tr class="category-col section" id="<?= $group ?>">
                                    <td>
                                        <h2><?= $group ?></h2>
                                    </td>
                                </tr>
                                <?php foreach ($specs as $spec) : ?>
                                    <tr class="price-col-attribute">
                                        <td><?= $spec['label'] ?></td>
                                    </tr>
                                <?php endforeach; ?>
                            <?php endforeach; ?>
                        </tbody>

                    </table>
                </div>

                <?php foreach ($variant_spec_data as $variant_id => $variant_data) : ?>
                    <div id="<?= $variant_id ?>" class="sectioning">
                        <table>
                            <thead>
                                <tr class="header-col-data">
                                    <td class="body-col-span">
                                        <div class="button-pin">
                                            <button class="pin-button">📌</button>
                                            <button class="close-button" title="Remove this table">✖</button>
                                        </div>
                                        <div class="header_button">
                                            <div class="varient-id" data-tooltip="Your tooltip text here">
                                                <span><?= $variant_id ?> </span>
                                            </div>
                                            <button class="body-col-span-button">+ Compare</button>

                                        </div>
                                    </td>
                                </tr>
                            </thead>
                            <tbody>
                                <?php foreach ($variant_spec_groups as $group => $specs) : ?>
                                    <tr class="body-category-col">
                                        <td></td>
                                    </tr>
                                    <?php foreach ($specs as $spec) : ?>
                                        <tr class="price-col-attribute-data">
                                            <td><?= $variant_data[$group][$spec['key']] ?></td>
                                        </tr>
                                    <?php endforeach; ?>
                                <?php endforeach; ?>
                            </tbody>
                        </table>
                    </div>
                <?php endforeach; ?>
            </div>
        </div>
    </div>
    </div>

    <style>
        .varient-id {
            display: -webkit-box;
            -webkit-line-clamp: 1;
            -webkit-box-orient: vertical;
            overflow: hidden;
            text-overflow: ellipsis;
            position: relative;
            cursor: pointer;
        }

        .pin-button {
            margin-left: -10px;
            background: none;
            border: none;
            color: #ddd;
        }

        .button-pin {
            display: flex;
            justify-content: space-between;

        }

        thead {
            position: sticky;
            top: 162px;
            background: #fff;
            z-index: 100;
            box-shadow: 0 1px 1px rgba(0, 0, 0, 0.1);
        }

        .close-button {
            background: none;
            border: none;
            color: #ddd;
            font-weight: 100;
        }

        .comparison-content .header-col {
            z-index: 999;
            display: flex;
            width: 250px;
            height: 136px;
            padding: 30px 20px 20px 20px;
            position: sticky;
            top: 0px;
            background: #fff;
            box-shadow: 0 1px 1px rgba(0, 0, 0, 0.1);
            border-right: 1px solid #ddd;
            align-content: center;
            justify-content: center;
            align-items: center;
        }

        .comparison-content .header-col-data {
            z-index: 999;
            display: flex;
            width: 250px;
            height: 136px;
            position: sticky;
            top: 150px;
            background: #fff;
            box-shadow: 0 1px 1px rgba(0, 0, 0, 0.1);
            border-right: 1px solid #ddd;
            align-content: center;
            justify-content: center;
            align-items: center;
        }

        .comparison-content .category-col {
            height: 65px;
            border-top: 1px solid #ddd;
            border-right: 0px solid #ddd;
            border-bottom: 1px solid #ddd;

        }

        .comparison-content .body-category-col {
            height: 65px;
            border-top: 1px solid #ddd;
            border-left: 0px solid #ddd;
            border-right: 0px solid #ddd;
            border-bottom: 1px solid #ddd;
        }

        .header-col-span {
            border: none;
            vertical-align: middle;
        }

        .body-col-span {
            border: none;
            vertical-align: middle;
            border: none;
            width: 250px;
        }

        .comparison-content .price-col-attribute {
            background: #fff;
            padding: 14px;
            border-right: 1px solid #ddd;
            border-bottom: 1px solid #ddd;
            border-top: none;
            color: #9d9d9d;
        }

        .comparison-content .price-col-attribute td {
            font-weight: 500;
        }

        .comparison-content .price-col-attribute-data {
            background: #fff;
            padding: 14px;
            border-bottom: 1px solid #ddd;
            border-right: 1px solid #ddd;
            border-top: none;
            color: #000000;
        }

        .comparison-content .price-col-attribute-data td {
            font-weight: 500;
        }

        .header_button {
            display: flex;
            align-items: center;
            margin-bottom: 20px;
            flex-direction: column;
            font-size: 15px;
            color: black;
            position: relative;
        }

        button.body-col-span-button {
            display: flex;
            justify-content: center;
            align-items: center;
            width: 100%;
            border-radius: 4px;
            border-color: #32d0c6;
            font-size: 15px;
            font-weight: 400;
            font-family: "Roboto";
            color: #32d0c6;
            line-height: 20px;
            text-align: center;
            padding: 8px 44px;
            transition: all .1s;
            cursor: pointer;
            border: 2px solid;
            background-color: white;
            margin-top: 20px;
        }

        .header-col-span .toggle-switch {
            display: flex;
            align-items: center;
            margin-bottom: 20px;
            flex-direction: column;
            color: black;
            font-size: 19px;
        }

        .header-col-span .switch {
            position: relative;
            display: inline-block;
            width: 40px;
            height: 20px;
            margin-right: 10px;
        }

        .header-col-span .switch input {
            display: none;
        }

        .header-col-span .specs-slider {
            position: absolute;
            cursor: pointer;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-color: #ccc;
            transition: 0.4s;
            border-radius: 20px;
        }

        .header-col-span .specs-slider:before {
            position: absolute;
            content: "";
            height: 14px;
            width: 14px;
            left: 3px;
            bottom: 3px;
            background-color: white;
            transition: 0.4s;
            border-radius: 50%;
        }

        .header-col-span input:checked+.specs-slider {
            background-color: #00bcd4;
        }

        .header-col-span input:checked+.specs-slider:before {
            transform: translateX(20px);
        }

        .simple-table {
            background-color: #f5f5f5;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: none;
        }

        .table-section {
            margin-bottom: 20px;
            border: none;

        }

        .table-section h3 {
            margin-bottom: 10px;
            font-size: 16px;
            font-weight: bold;
            color: #000000;
            padding-left: 10px;
            border: none;
        }

        .table-section ul {
            list-style-type: none;
            padding: 0;
            margin: 0;
            border: none;

        }

        .table-section ul li {
            padding: 8px 0;
            border: none;
        }

        .table-section ul li:last-child {
            border: none;
        }

        .comparison-container {
            display: flex;
            position: relative;
            font-family: "Roboto";
        }

        .sidebar-menu {
            width: 300px;
            background-color: #ffffff;
            padding: 10px;
            position: -webkit-sticky;
            position: sticky;
            top: 150px;
            align-self: flex-start;
            max-height: calc(100vh - 40px);
            overflow-y: auto;
            margin-top: 140px;
        }

        .sidebar-menu ul {
            list-style-type: none;
            padding: 0;
            margin: 0px;
        }

        .sidebar-menu ul li {
            padding: 3px;
            cursor: pointer;
            color: #8c8c8c;
            font-size: 13px;
            text-align: center;
            margin: 5px 0px;
            background-color: #f9f9f9;
            border-radius: 4px 0 0 4px;
        }

        .sidebar-menu ul li a.active {
            background-color: #28d7d4;
            color: #fff;
            position: relative;
        }

        .sidebar-menu ul li.active:hover {
            color: #fff;
        }

        .sidebar-menu ul li.active::after {
            content: '';
            position: absolute;
            background: linear-gradient(to right bottom, #32d0c6 50%, transparent 50%);
            right: -10px;
            top: 50%;
            transform: translateY(-50%);
            border-left: 20px solid #28d7d4;
            border-top: 15px solid transparent;
            border-bottom: 15px solid transparent;
        }

        .sidebar-menu ul li:hover {
            color: #28d7d4;
        }

        .comparison-content {
            flex-grow: 1;
            background-color: #fff;
            margin-left: 20px;
            display: flex;
            border: 1px solid #ddd;
        }

        .sectioning {
            margin-bottom: -25 px;
        }

        .sectioning h2 {
            font-size: 19px;
            color: black;
            border: none;
            margin: 8px 0px 8px 0px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            border: none;
        }

        table td {
            padding: 10px;
            font-size: 16px;
            border: none;
        }

        table td:first-child {
            font-weight: bold;
            border: none;
        }

        html {
            scroll-behavior: smooth;
        }

        .menu-item.active {
            background-color: #28d7d4;
            color: #ffffff;
        }

        .menu-item.active:hover {
            color: #ffffff !important;
        }

        .sidebar-menu li {
            position: relative;
        }

        .sidebar-menu li a {
            display: block;
            width: 100%;
            height: 100%;
            padding: 3px;
            text-decoration: none;
            color: inherit;
        }
    </style>
    <script>
        document.querySelectorAll('.sidebar-menu li').forEach(item => {
            item.addEventListener('click', function(e) {
                if (e.target.tagName !== 'A') {
                    e.preventDefault();
                }
            });
        });

        document.addEventListener('DOMContentLoaded', function() {
            const menuLinks = document.querySelectorAll('.sidebar-menu li a');
            const sections = document.querySelectorAll('.category-col');
            const offset = 200;
            let isClicking = false;

            function updateActiveMenuOnScroll() {
                if (isClicking) return;
                sections.forEach((section, index) => {
                    const rect = section.getBoundingClientRect();
                    if (rect.top >= 0 && rect.top <= 200) {
                        menuLinks.forEach((link) => link.classList.remove('active'));
                        menuLinks[index].classList.add('active');
                    }
                });
            }

            menuLinks.forEach((link, index) => {
                link.addEventListener('click', function(e) {
                    e.preventDefault();
                    isClicking = true;

                    const sectionTop = sections[index].getBoundingClientRect().top + window.pageYOffset - offset;
                    window.scrollTo({
                        top: sectionTop,
                        behavior: 'smooth'
                    });

                    menuLinks.forEach((link) => link.classList.remove('active'));
                    link.classList.add('active');

                    setTimeout(() => {
                        isClicking = false;
                    }, 500);
                });
            });

            window.addEventListener('scroll', updateActiveMenuOnScroll);
            updateActiveMenuOnScroll();
        });

        document.addEventListener("DOMContentLoaded", function() {
            const menuItems = document.querySelectorAll(".sidebar-menu li");
            const closeButtons = document.querySelectorAll('.close-button');
            closeButtons.forEach(button => {
                button.addEventListener('click', function() {
                    const tableSection = this.closest('.sectioning');
                    if (tableSection) {
                        tableSection.remove();
                    }
                });
            });

            const pinButtons = document.querySelectorAll(".pin-button");
            pinButtons.forEach((button, index) => {
                button.addEventListener("click", function() {
                    const tableToPin = button.closest(".sectioning");
                    const allTables = document.querySelectorAll('.sectioning');

                    if (tableToPin && allTables.length > 1) {
                        const parent = tableToPin.parentNode;
                        parent.insertBefore(tableToPin, allTables[1]);
                    }
                });
            });

        });
    </script>

<?php

    return ob_get_clean();
}
