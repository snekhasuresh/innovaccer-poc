<?php

// import single-listing-specs.css from ./css/single-listing-specs.css
function enqueue_motor_single_listing_specs_styles()
{
    wp_enqueue_style('single-listing-specs', get_stylesheet_directory_uri() . '/widget-shortcodes/css/single-listing-specs.css');
}

function display_motor_custom_page()
{
    enqueue_motor_single_listing_specs_styles();

    $global_listing_post_data = get_motor_listing_from_query_vars();
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
        'ราคา' => [['label' => 'Price', 'key' => 'price']],
        'ต้นทุน' => [
            ['label' => 'การชำระเงินรายเดือน', 'key' => 'monthly_payment']
        ],
        'สเปคหลัก' => [
            ['label' => 'Brand', 'key' => 'make'],
            ['label' => 'Model', 'key' => 'model'],
            ['label' => 'ประเภท', 'key' => 'body_type'],
            ['label' => 'Launch Time', 'key' => 'year'],
            ['label' => 'เครื่องยนต์', 'key' => 'capacity_main'],
            ['label' => 'กำลังไฟสูงสุด(พีเอส)', 'key' => 'maximum_power'],
            ['label' => 'ตัวเลือกการเปิดเครื่องยนต์', 'key' => 'engine_opening_option'],
            ['label' => 'On Sale', 'key' => 'on_sale'],
            ['label' => 'รูปแบบเกียร์', 'key' => 'gear_box'],
            ['label' => 'ประเภทน้ำมันเชื้อเพลิง', 'key' => 'fuel_type'],
            ['label' => 'อัตราสิ้นเปลืองน้ำมันเชื้อเพลิง(L/100km)', 'key' => 'fuel_consumption'],
        ],
        'เครื่อง&คุณสมบัติ' => [
            ['label' => 'ปริมาตรกระบอกสูบ (ซีซี)', 'key' => 'capacity'],
            ['label' => 'แบบเครื่องยนต์', 'key' => 'engine_type'],
            ['label' => 'แรงบิดสูงสุด(นิวตัน-เมตร)', 'key' => 'maximum_torque'],
            ['label' => 'กำลังไฟสูงสุด(พีเอส)', 'key' => 'maximum_power'],
            ['label' => 'รอบต่อนาที ณ ความแรงเครื่องสูงสุด(รอบต่อนาที)', 'key' => 'rpm_at_max_engine_strength'],
            ['label' => 'ความเร็วสูงสุด', 'key' => 'speed'],
            ['label' => 'จำนวนของกระบอกสูบ', 'key' => 'number_of_cylinders'],
            ['label' => 'ชนิดของคลัทช์', 'key' => 'clutch_type'],
            ['label' => 'รอบต่อนาที ณ ความแรงบิดสูงสุด(รอบต่อนาที)', 'key' => 'maximum_power_speed'],
            ['label' => 'ความจุถังน้ำมันเชื้อเพลิง', 'key' => 'fuel_tank_capacity'],
        ],
        'ขนาด' => [
            ['label' => 'ความยาว', 'key' => 'length'],
            ['label' => 'ขนาด (ยาวxกว้างxสูง มม.)', 'key' => 'size'],
            ['label' => 'ความกว้าง', 'key' => 'width'],
            ['label' => 'ความสูง', 'key' => 'height'],
            ['label' => 'ความสูงใต้ท้องรถ', 'key' => 'height_under_the_car'],
            ['label' => 'ฐานล้อ', 'key' => 'wheelbase'],
            ['label' => 'น้ำหนักตัวรถ', 'key' => 'car_body_weight'],
            ['label' => 'ความสูงที่นั่ง', 'key' => 'seat_height'],
            ['label' => 'ความจุของถังน้ำมัน', 'key' => 'oil_tank_capacity'],
            ['label' => 'จำนวนที่นั่ง', 'key' => 'seat'],
            ['label' => 'ขนาดเครื่องยนต์', 'key' => 'capacity'],
        ],
        'ระบบเกียร์' => [
            ['label' => 'ระบบขับเคลื่อน', 'key' => 'drive_system'],
            ['label' => 'ระบบเกียร์', 'key' => 'gearbox'],
        ],
        'แชสซี&ช่วงล่าง' => [
            ['label' => 'ระบบกันสะเทือนด้านหน้า', 'key' => 'front_suspension'],
            ['label' => 'ระบบกันสะเทือนด้านหลัง', 'key' => 'rear_suspension'],
        ],
        'ระบบไฟฟ้า' => [
            ['label' => 'ไฟหน้ารถ', 'key' => 'headlight'],
            ['label' => 'โคมไฟด้านหลัง', 'key' => 'lamp_back'],
            ['label' => 'ระบบปรับไฟหน้า สูง / ต่ำ', 'key' => 'high_low_headlight_adjustment_system'],
            ['label' => 'ไฟแสดงสถานะ', 'key' => 'indicator_light'],
        ],
        'ระบบควบคุม' => [
            ['label' => 'ระบบล๊อกรอบออกตัว', 'key' => 'frame_lock_system'],
            ['label' => 'สวิทช์ปรับไฟสูง', 'key' => 'high_beam_adjustment_switch'],
            ['label' => 'มาตรวัดระยะทาง', 'key' => 'odometer'],
            ['label' => 'หน้าจอแสดงผล', 'key' => 'display_screen'],
            ['label' => 'ระบบนำทางเนวิเกเตอร์', 'key' => 'navigation'],
            ['label' => 'เครื่องวัดความเร็วรอบ', 'key' => 'tachometer'],
            ['label' => 'ระบบเปิด-ปิด เครื่องยนต์ด้วยกุญแจอัจฉริยะ', 'key' => 'smart_key_engine_on-off_system'],
        ],
        'ล้อ&ยาง' => [
            ['label' => 'ประเภทยางรถยนต์', 'key' => 'tire_type'],
            ['label' => 'ขนาดของล้อหน้า', 'key' => 'front_wheel_size'],
            ['label' => 'ระบบกันสะเทือน', 'key' => 'suspension'],
            ['label' => 'ขนาดของล้อหลัง', 'key' => 'rear_wheel_size'],
            ['label' => 'ยางล้อหน้า', 'key' => 'front_tire'],
            ['label' => 'ยางล้อหลัง', 'key' => 'rear_tire'],
        ],
        'เบรค' => [
            ['label' => 'เบรคหน้า', 'key' => 'front_brake'],
            ['label' => 'ด้านหลังเบรค', 'key' => 'rear_brake'],
        ],
        'ความปลอดภัย' => [
            ['label' => 'ตัวบอกสถานะการเปลี่ยนน้ำมันเครื่อง', 'key' => 'engine_oil_change_indicator'],
            ['label' => 'ระบบควบคุุมเสถียรภาพการทรงตัวของรถ', 'key' => 'system_to_control_the_stability_of_the_vehicle'],
            ['label' => 'เอบีเอส หรือ ระบบป้องกันเบรคจนล้อล๊อคตาย', 'key' => 'abs'],
            ['label' => 'ระบบป้องกันการลื่นไถลของรถ', 'key' => 'anti-skid_system'],
        ],
        'ฟีเจอร์พิเศษ' => [
            ['label' => 'ระบบควบคุมความเร็วอัตโนมัติ', 'key' => 'automatic_speed_control'],
            ['label' => 'การปรับช่วงล่างด้วยระบบไฟฟ้า', 'key' => 'adjusting_the_suspension_electrically'],
            ['label' => 'เบาะที่นั่งปรับความสูงได้', 'key' => 'height_adjustable_seat_cushion'],
            ['label' => 'โหมดการขับขี่', 'key' => 'driving_mode'],
        ],
    ];

    $variant_spec_data = array();

    foreach ($variant_posts as $variant_post) {
        $variant_meta = $all_variants_meta[$variant_post->ID];

        foreach ($variant_spec_groups as $group => $specs) {
            $variant_spec_data[$variant_post->post_title][$group] = [];

            foreach ($specs as $spec) {
                $key = $spec['key'];

                if ($group == 'สเปคหลัก') {
                    switch ($spec['key']) {
                        case 'make':
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $make;
                            break;

                        case 'model':
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $listing_post->post_title;
                            break;

                        case 'body_type':
//                             $body_type = $listing_meta['listing_type'][0];
//                             $body_type = get_term_by('id', $body_type, 'motorcycle-listing-type')->name;
                             // Retrieve the serialized data from the listing_type
							// Example serialized string
								$serialized_string =  $listing_meta['listing_type'][0];

								// Use a regex to extract all IDs
								preg_match_all('/s:\d+:"(\d+)";/', $serialized_string, $matches);

								// Check if IDs were found
								if (!empty($matches[1])) {
									foreach ($matches[1] as $id) {
										// Fetch the term name using the ID
										$body_type = get_term_by('id', $id, 'motorcycle-listing-type');
										$body_type_name = $body_type->name;
									}
								} else {
									$body_type_name = '--';
								}

                            $variant_spec_data[$variant_post->post_title][$group][$key] = $body_type_name;
                            break;

                        default:
                            $value = isset($variant_meta[$key][0]) ? $variant_meta[$key][0] : '-';
                            $variant_spec_data[$variant_post->post_title][$group][$key] = $value;
                            break;
                    }
                } else {
                    $value = isset($variant_meta[$key][0]) ? $variant_meta[$key][0] : '-';
                    $price_keys = ['price', 'monthly_payment'];
                    if (in_array($key, $price_keys)) {
                        if (($value == '0' || $value == '-')) {
                            $value = 'ยังไม่คอนเฟิร์ม';
                        } else {
                            $value = 'THB ' .format_number_with_commas($value);
                        }
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
                                        <span>ซ่อนข้อมูลเดียวกัน</span>
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
                                                <a href="<?php echo home_url('/compare-motorcycles'); ?>">+ เปรียบเทียบ</a>
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
add_shortcode('motor_specs_shortcode', 'display_motor_custom_page');
