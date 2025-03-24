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
        'Giá' => [['label' => 'Giá', 'key' => 'price']],
        'Chi Phí' => [
            ['label' => 'Trả Góp', 'key' => 'monthly_payment']
        ],
        'Thông số kỹ thuật quan trọng' => [
            ['label' => 'Thương hiệu', 'key' => 'make'],
            ['label' => 'Dòng xe', 'key' => 'model'],
            ['label' => 'Công suất tối đa(PS)', 'key' => 'maximum_power'],
            ['label' => 'Năm sản xuất', 'key' => 'year'],
            ['label' => 'Loại động cơ', 'key' => 'engine_type'],
			['label' => 'Bắt đầu các tùy chọn', 'key' => 'start_option'],
			['label' => 'Loại', 'key' => 'body_type'],
            ['label' => 'Khuyến mãi', 'key' => 'on_sale'],
            ['label' => 'Mức tiêu thụ nhiên liệu(L/100km)', 'key' => 'fuel_consumption'],
            ['label' => ' Kiểu truyền tải', 'key' => 'transmission'],
            ['label' => 'Loại nhiên liệu', 'key' => 'fuel_type'],
        ],
        'Động cơ và hiệu suất' => [
            ['label' => 'Tốc độ tối đa', 'key' => 'maximum_speed'],
            ['label' => 'Mô-men xoắn cực đại RPM (RPM)', 'key' => 'rpm_maximum_torque'],
            ['label' => 'số xi lanh', 'key' => 'number_of_cylinders'],
            ['label' => 'Công suất tối đa RPM (RPM)', 'key' => 'rpm_maximum_power'],
            ['label' => 'Mô-men xoắn cực đại(Nm)', 'key' => 'maximum_torque'],
            ['label' => 'Số kì', 'key' => 'number_of_strokes'],
            ['label' => 'Dung tích(cc)', 'key' => 'capacity'],
        ],
        'Kích thước' => [
            ['label' => 'Dài(mm)', 'key' => 'length'],
            ['label' => 'Cao(mm)', 'key' => 'height'],
            ['label' => 'Rộng(mm)', 'key' => 'width'],
            ['label' => 'Trọng lượng(kg)', 'key' => 'weight'],
            ['label' => 'Yên xe', 'key' => 'seat'],
            ['label' => 'Dung tích bình xăng', 'key' => 'fuel_tank_capacity'],
        ],
        'Bánh răng và hộp số' => [
            ['label' => 'Hộp số', 'key' => 'gear_box'],
            ['label' => 'Kiểu truyền tải', 'key' => 'transmission'],
            ['label' => 'Loại ổ', 'key' => 'jenis_penggerak'],
			
        ],
        'Loại khung và hệ thống treo' => [
            ['label' => 'Khoảng sáng gầm xe', 'key' => 'ground_clearance'],
            ['label' => 'Chiều cao yên', 'key' => 'chair_height'],
            ['label' => 'Hệ thống treo sau', 'key' => 'rear_suspension'],
            ['label' => 'Hệ thống treo trước', 'key' => 'front_suspension'],
            ['label' => 'Điều chỉnh hệ thống treo điện tử', 'key' => 'electronic_suspension_adjustment'],
			
        ],
        'Hệ thống điện' => [
            ['label' => 'Đầu đèn', 'key' => 'head_lamp'],
            ['label' => 'Đèn xi nhan', 'key' => 'indicator_light'],
            ['label' => 'Đèn sau xe', 'key' => 'taillight'],
        ],
        'Bảng điều khiển lưu trữ' => [
            ['label' => 'Chỉ báo nhiên liệu', 'key' => 'bbm_indicator'],
            ['label' => 'Công tơ mét', 'key' => 'speedometer'],
            ['label' => 'Đèn báo thay dầu', 'key' => 'oil_change_indicator'],
            ['label' => 'Bảng điều khiển', 'key' => 'instrument_panel'],
            ['label' => 'Đồng hồ tua máy', 'key' => 'tachometer'],
            ['label' => 'Màn hình hiển thị', 'key' => 'display_screen'],
            ['label' => 'Công tắc điều chỉnh độ sáng', 'key' => 'dimmer_switch'],
            ['label' => 'Khóa trung tâm', 'key' => 'central_locking'],
			
        ],
        'Kích thước bánh xe và lốp' => [
            ['label' => 'Kích thước bánh sau', 'key' => 'rear_wheel_size'],
            ['label' => 'Kích thước bánh trước', 'key' => 'front_wheel_size'],
            ['label' => 'lốp trước', 'key' => 'front_tire'],
            ['label' => 'Lốp sau', 'key' => 'rear_tire'],
            ['label' => 'Loại lốp', 'key' => 'tire_type'],
        ],
        'Phanh' => [
            ['label' => 'Phanh trước/Thắng trước', 'key' => 'front_brake'],
            ['label' => 'Phanh sau/thắng sau', 'key' => 'rear_brake'],
        ],
        'Các tính năng an toàn và bảo mật' => [
            ['label' => 'Hệ thống chống bó cứng phanh', 'key' => 'abs'],
            ['label' => 'Hệ thống chống trôm', 'key' => 'immobilizer'],
            ['label' => 'Kiểm soát ổn định', 'key' => 'stability_control'],
            ['label' => 'Cảnh báo kiểm tra động cơ', 'key' => 'engine_check_warning'],
            ['label' => 'Báo thức', 'key' => 'alarm'],
            ['label' => 'Khóa bánh trước / sau', 'key' => 'front__rear_wheel_lock'],
            ['label' => 'Đèn xi nhan', 'key' => 'side_standard_indicator'],
            ['label' => 'Kiểm soát lực kéo', 'key' => 'traction_control'],
        ],
        'Các tính năng đặc biệt hiện có' => [
            ['label' => 'Chế độ lái', 'key' => 'driving_mode'],
            ['label' => 'Kiểm soát hành trình', 'key' => 'cruise_control'],
            ['label' => 'Đèn pha có thể điều chỉnh', 'key' => 'adjustable_headlights'],
        ],
    ];

    $variant_spec_data = array();

    foreach ($variant_posts as $variant_post) {
        $variant_meta = $all_variants_meta[$variant_post->ID];

        foreach ($variant_spec_groups as $group => $specs) {
            $variant_spec_data[$variant_post->post_title][$group] = [];

            foreach ($specs as $spec) {
                $key = $spec['key'];

                if ($group == 'Thông số kỹ thuật quan trọng') {
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
                            $value = 'Đang cập nhật';
                        } else {
                            $value = format_price_vietnam($value);
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
                                        <span>Ẩn giống nhau</span>
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
                                            <button class="body-col-span-button" onclick="window.location.href='<?php echo esc_url(home_url('/so-sanh-xe-may')); ?>'">
                                                <a href="<?php echo home_url('/so-sanh-xe-may'); ?>">+  So sánh </a>
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
