<?php

// import single-listing-specs.css from ./css/single-listing-specs.css
function enqueue_single_listing_specs_styles()
{
    wp_enqueue_style('single-listing-specs', get_stylesheet_directory_uri() . '/widget-shortcodes/css/single-listing-specs.css');
}

function display_custom_page()
{
    enqueue_single_listing_specs_styles();

    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data) {
        return;
    }

    $make = $global_listing_post_data['listing_make_term']->name;
    $listing_post = $global_listing_post_data['post'];
    $listing_meta = $global_listing_post_data['post_meta'];
    $variant_posts = $global_listing_post_data['variant_posts'];
    $all_variants_meta = $global_listing_post_data['variant_meta_data'];

    if (empty($variant_posts)) {
        return;
    }
	
	$variant_posts = array_slice($variant_posts, 0, 4);

    $variant_spec_groups = [
        'Price' => [['label' => 'Retail Price(RM)', 'key' => 'retail_price']],
        'Costs' => [
            ['label' => 'Insurance(RM)', 'key' => 'insurance'],
            ['label' => 'Road Tax(RM)', 'key' => 'road_tax'],
            ['label' => 'Monthly Payment(RM)', 'key' => 'monthly_payment']
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
            ['label' => 'Power Consumption (kWh/100km)', 'key' => 'power_consumption_per_100km'],
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
        $variant_meta = $all_variants_meta[$variant_post->ID];

        foreach ($variant_spec_groups as $group => $specs) {
            $variant_spec_data[$variant_post->post_title][$group] = [];

            foreach ($specs as $spec) {
                $key = $spec['key'];

                if ($group == 'Overview') {
                    switch ($spec['key']) {
                        case 'make':
                            $variant_spec_data[$variant_post->post_title][$group][$key] = ucfirst($make);
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
                    $price_keys = ['retail_price', 'insurance', 'road_tax', 'monthly_payment'];
                    if (in_array($key, $price_keys)) {
                        if (($value == '0' || $value == '-')) {
                            $value = 'TBC';
                        } else {
                            $value = format_number_with_commas($value);
                        }
                    }

                    $yesnokeys = ['driverfront_seat_passenger_airbags', 'frontrear_side_airbags', 'frontrear_curtain_airbags'];
                    if (in_array($key, $yesnokeys)) {
                        $value = convertYesNo($value);
                    }

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
                                            <button class="body-col-span-button" onclick="window.location.href='<?php echo esc_url(home_url('/compare-cars')); ?>'">
                                                <a href="<?php echo home_url('/compare-cars'); ?>">+ Compare</a>
                                            </button>

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

function convertYesNo($value)
{
    $parts = explode('/', $value);
    $converted_parts = array_map(function ($part) {
        return trim($part) === 'Yes' ? 'Y' : (trim($part) === 'No' ? 'N' : $part);
    }, $parts);

    return implode('/', $converted_parts);
}
