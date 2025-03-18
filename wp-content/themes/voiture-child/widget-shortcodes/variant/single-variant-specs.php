<?php

add_shortcode('single_variant_specs', 'single_variant_specs_shortcode');

// import single-listing-specs.css from ./css/single-listing-specs.css
function enqueue_single_variant_specs_styles()
{
    wp_enqueue_style('single-listing-specs', get_stylesheet_directory_uri() . '/widget-shortcodes/css/single-listing-specs.css');
}

function single_variant_specs_shortcode()
{
    enqueue_single_variant_specs_styles();

    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');
    $listing_name = $make . '-' . $listing_name;

    if (strpos($section, $listing_name) === false) {
        $section = $listing_name . '-' . $section;
    }

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
        'name' => $section
    );
    $variants = new WP_Query($args);

    // if no variants found, return
    if (!$variants->have_posts()) {
        return;
        wp_die('No Variants Found');
    }

    $variant_posts = $variants->posts;

    $variant_spec_groups = [
        'Giá' => [['label' => 'Giá', 'key' => 'retail_price']],
        'Chi Phí' => [
            ['label' => 'Bảo Hiểm', 'key' => 'insurance'],
            ['label' => 'Trả Góp', 'key' => 'monthly_payment']
        ],
        'Thông số cơ bản' => [
            ['label' => 'Thương hiệu', 'key' => 'make'],
            ['label' => 'Mẫu xe', 'key' => 'model'],
            ['label' => 'Biến thể', 'key' => 'variant_name'],
            ['label' => 'Loại xe', 'key' => 'body_type'],
            ['label' => 'Phân khúc', 'key' => 'segment'],
            ['label' => 'Loại năng lượng', 'key' => 'fuel_type'],
            ['label' => 'Năm sản xuất', 'key' => 'launched_year'],
            ['label' => 'Công suất(PS)', 'key' => 'horsepower'],
            ['label' => 'Mô-men xoắn cực đại(Nm)', 'key' => 'torque'],
            ['label' => 'Công suất động cơ đốt trong (PS)', 'key' => 'engine_power'],
            //             ['label' => 'Công suất mô-tơ điện(PS)', 'key' => 'engine_power'],
            ['label' => 'Kích thước tổng thể DxRxC', 'key' => 'length_weight_height'],
            ['label' => 'Dung tích bình xăng (lít)', 'key' => 'fuel_tank'],
            ['label' => 'Mức tiêu thụ nhiên liệu', 'key' => 'manufacturers_claim'],
            ['label' => 'Khuyến mãi', 'key' => 'on_sale'],
            ['label' => 'Hộp số', 'key' => 'gearbox'],
        ],
        'Động cơ' => [
            ['label' => 'Động cơ', 'key' => 'engine'],
            ['label' => 'Hệ thống nạp', 'key' => 'loading_system'],
            ['label' => 'Dung tích xy lanh(L)', 'key' => 'capacity_format'],
            ['label' => 'Công suất động cơ đốt trong (PS)', 'key' => 'combustion_engine_power'], //need to change name
            ['label' => 'Công suất động cơ đốt trong(kW)', 'key' => 'horsepower_kw'],
            ['label' => 'Dung tích xy lanh(cc)', 'key' => 'capacity'],
            ['label' => 'Mô-men xoắn từ động cơ đốt trong', 'key' => 'combined_engine_torque'], //problem
        ],
        'Kích thước' => [
            ['label' => 'Dài(mm)', 'key' => 'length'],
            ['label' => 'Cao(mm)', 'key' => 'height'], //not there
            ['label' => 'Rộng(mm)', 'key' => 'width'],
            ['label' => 'Kích thước tổng thể DxRxC', 'key' => 'length_weight_height'],
            ['label' => 'Trục cơ sở(mm)', 'key' => 'wheelbase'],
            ['label' => 'Trọng lượng (kg)', 'key' => 'weight'],
            ['label' => 'Khoảng sáng gầm(mm)', 'key' => 'ground_clearance'], //not there
            ['label' => 'Bán kính vòng quay tối thiểu(m)', 'key' => 'minimum_turning_radius'],
            ['label' => 'Cửa xe', 'key' => 'doors'],
            ['label' => 'Ghế ngồi', 'key' => 'seats'],
            ['label' => 'Dung tích khoang chứa đồ (lít)', 'key' => 'storage_compartment_capacity'],
        ],
        'Hộp số & Khung xe' => [
            ['label' => 'Hộp số', 'key' => 'transmission'],
            ['label' => 'Lốp trước', 'key' => 'front_tyres'],
            ['label' => 'Lốp sau', 'key' => 'rear_tyres'],
            ['label' => 'Kích thước La zăng', 'key' => 'wheel_size'],
            ['label' => 'Hệ thống treo trước', 'key' => 'front_suspension'],
            ['label' => 'Hệ thống treo sau', 'key' => 'rear_suspension'],
            ['label' => 'Trợ lực lái', 'key' => 'steering'],
        ],
        'Ngoại thất' => [
            ['label' => 'Cụm đèn trước', 'key' => 'front_light_cluster'],
            ['label' => 'Cụm đèn sau', 'key' => 'rear_light_cluster'],
            ['label' => 'Đèn sương mù', 'key' => 'fog_lights'],
            ['label' => 'Gương gập điện', 'key' => 'folding_wing_mirror'],
            ['label' => 'Gương chiếu hậu chống chói', 'key' => 'Anti_glare_rearview_mirror'],
        ],
        'Chasis' => [
            ['label' => 'Chất liệu nội thất', 'key' => 'interior_material'],
            ['label' => 'Điều chỉnh ghế lái', 'key' => 'adjusting_the_driver_seat'],
            ['label' => 'Phanh tay điện tử', 'key' => 'electronic_handbrake'],
            ['label' => 'Điều hòa tự động', 'key' => 'air_conditioning_system'],
            ['label' => 'Điều hòa sau', 'key' => 'rear_air_conditioner'],
            ['label' => 'Màn hình LCD', 'key' => 'lcd_screen'],
            ['label' => 'Apple Carplay và Android Auto', 'key' => 'apple_carplay_and_android_auto'],
            ['label' => 'Hệ thống loa', 'key' => 'speakers'],
            ['label' => 'Hệ thống âm thanh', 'key' => 'sound_plus_functions'],
        ],
        'An toàn' => [
            ['label' => 'Túi khí', 'key' => 'airbags'],
            ['label' => 'Tùy chọn chế độ lái', 'key' => 'select_running_mode'],
            ['label' => 'Kiểm soát hành trình', 'key' => 'cruise_control'],
            ['label' => 'Chống bó cứng phanh (ABS)', 'key' => 'abs'],
            ['label' => 'Hỗ trợ phanh khẩn cấp (BA)', 'key' => 'ba'],
            ['label' => 'Cân bằng điện tử (ESP)', 'key' => 'esp'],
            ['label' => 'Phân bổ lực phanh điện tử (EBD)', 'key' => 'ebd'],
            ['label' => 'Trợ lực điện (EPS)', 'key' => 'eps'],
            ['label' => 'Camera lùi', 'key' => 'reverse_camera'],
            ['label' => 'Cảm biến đỗ xe sau', 'key' => 'rear_parking_assist_sensor'],
            ['label' => 'Cảnh báo điểm mù', 'key' => 'blind_spot_info_system'],
        ],
    ];



    $variant_spec_data = array();

    foreach ($variant_posts as $variant_post) {
        $variant_meta = get_post_meta($variant_post->ID);

        foreach ($variant_spec_groups as $group => $specs) {
            $variant_spec_data[$variant_post->post_title][$group] = [];

            foreach ($specs as $spec) {
                $key = $spec['key'];

                if ($group == 'hông số cơ bản') {
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
                            $value = 'Đang cập nhật';
                        } else {
                            $value = format_price_vietnam($value);
                        }
                    }

                    $yesnokeys = ['driverfront_seat_passenger_airbags', 'frontrear_side_airbags', 'frontrear_curtain_airbags'];
                    if (in_array($key, $yesnokeys)) {
                        $value = motorconvertYesNo($value);
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
                                        <span style="white-space: nowrap;">Ẩn giống nhau</span>
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
                                            <button class="body-col-span-button" onclick="window.location.href='<?php echo esc_url(home_url('/so-sanh-xe')); ?>'">
                                                <a href="<?php echo home_url('/so-sanh-xe'); ?>">+  So sánh </a>
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

function motorconvertYesNo($value)
{
    $parts = explode('/', $value);
    $converted_parts = array_map(function ($part) {
        return trim($part) === 'Yes' ? 'Y' : (trim($part) === 'No' ? 'N' : $part);
    }, $parts);

    return implode('/', $converted_parts);
}
