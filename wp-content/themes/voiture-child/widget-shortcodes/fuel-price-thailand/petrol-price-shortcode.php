<?php

function fuel_price_template_shortcode()
{
    // Get all oil posts and their meta data
    $oil_posts = get_posts([
        'post_type' => 'oil',
        'numberposts' => -1,
    ]);

    // Get terms for petrol and diesel
    $petrol_term = get_term_by('slug', 'oil', 'fuel-type');
    $diesel_term = get_term_by('slug', 'diesel', 'fuel-type');
    $gasoline_term = get_term_by('slug', 'gas', 'fuel-type');

    // Filter petrol prices and diesel prices

    $petrol_prices = [];
    $diesel_prices = [];
    $gas_prices = [];

    foreach ($oil_posts as $oil_post) {
        $fuel_type = get_post_meta($oil_post->ID, 'fuel_type', true);
        // $price_change = get_post_meta($oil_post->ID, 'oil_price_change', true);
        // $color = get_post_meta($oil_post->ID, 'color', true);
        $oil_price = get_post_meta($oil_post->ID, 'oil_price', true);

        if ($fuel_type == $petrol_term->term_id) {
            $petrol_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
                // 'change' => $price_change,

            ];
        } elseif ($fuel_type == $diesel_term->term_id) {
            $diesel_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
                // 'change' => $price_change,

            ];
        }elseif ($fuel_type == $diesel_term->term_id) {
            $diesel_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
                // 'change' => $price_change,

            ];
        }elseif ($fuel_type == $gasoline_term->term_id) {
            $gas_prices[] = [
                'variant' => $oil_post->post_title,
                'price' => $oil_price,
                // 'change' => $price_change,

            ];
        }

    }


    $fuel_price_data = [
        ['วันที่' => '29.12.2023', 'เบนซิน_95' => '43.14', 'แก๊สโซฮอล์_95' => '35.25', 'แก๊สโซฮอล์_91' => '33.48', 'แก๊สโซฮอล์_E20' => '33.14', 'แก๊สโซฮอล์_E85' => '33.29', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '22.12.2023', 'เบนซิน_95' => '43.44', 'แก๊สโซฮอล์_95' => '35.55', 'แก๊สโซฮอล์_91' => '33.78', 'แก๊สโซฮอล์_E20' => '33.44', 'แก๊สโซฮอล์_E85' => '33.59', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '19.12.2023', 'เบนซิน_95' => '43.04', 'แก๊สโซฮอล์_95' => '35.15', 'แก๊สโซฮอล์_91' => '33.38', 'แก๊สโซฮอล์_E20' => '33.04', 'แก๊สโซฮอล์_E85' => '33.19', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '09.12.2023', 'เบนซิน_95' => '42.64', 'แก๊สโซฮอล์_95' => '34.75', 'แก๊สโซฮอล์_91' => '32.98', 'แก๊สโซฮอล์_E20' => '32.64', 'แก๊สโซฮอล์_E85' => '32.79', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '07.12.2023', 'เบนซิน_95' => '43.04', 'แก๊สโซฮอล์_95' => '35.15', 'แก๊สโซฮอล์_91' => '33.38', 'แก๊สโซฮอล์_E20' => '33.04', 'แก๊สโซฮอล์_E85' => '33.19', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '05.12.2023', 'เบนซิน_95' => '43.54', 'แก๊สโซฮอล์_95' => '35.65', 'แก๊สโซฮอล์_91' => '33.88', 'แก๊สโซฮอล์_E20' => '33.54', 'แก๊สโซฮอล์_E85' => '33.69', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '22.11.2023', 'เบนซิน_95' => '43.94', 'แก๊สโซฮอล์_95' => '36.05', 'แก๊สโซฮอล์_91' => '34.28', 'แก๊สโซฮอล์_E20' => '33.94', 'แก๊สโซฮอล์_E85' => '34.09', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '18.11.2023', 'เบนซิน_95' => '44.34', 'แก๊สโซฮอล์_95' => '36.45', 'แก๊สโซฮอล์_91' => '34.68', 'แก๊สโซฮอล์_E20' => '34.34', 'แก๊สโซฮอล์_E85' => '34.49', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '16.11.2023', 'เบนซิน_95' => '44.84', 'แก๊สโซฮอล์_95' => '37.05', 'แก๊สโซฮอล์_91' => '35.28', 'แก๊สโซฮอล์_E20' => '34.94', 'แก๊สโซฮอล์_E85' => '35.09', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '15.11.2023', 'เบนซิน_95' => '44.44', 'แก๊สโซฮอล์_95' => '36.65', 'แก๊สโซฮอล์_91' => '34.88', 'แก๊สโซฮอล์_E20' => '34.54', 'แก๊สโซฮอล์_E85' => '34.69', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '10.11.2023', 'เบนซิน_95' => '44.04', 'แก๊สโซฮอล์_95' => '36.25', 'แก๊สโซฮอล์_91' => '34.48', 'แก๊สโซฮอล์_E20' => '34.14', 'แก๊สโซฮอล์_E85' => '34.29', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '09.11.2023', 'เบนซิน_95' => '44.44', 'แก๊สโซฮอล์_95' => '36.65', 'แก๊สโซฮอล์_91' => '34.88', 'แก๊สโซฮอล์_E20' => '34.54', 'แก๊สโซฮอล์_E85' => '34.69', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '07.11.2023', 'เบนซิน_95' => '45.04', 'แก๊สโซฮอล์_95' => '37.25', 'แก๊สโซฮอล์_91' => '35.48', 'แก๊สโซฮอล์_E20' => '35.14', 'แก๊สโซฮอล์_E85' => '35.29', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '01.11.2023', 'เบนซิน_95' => '46.04', 'แก๊สโซฮอล์_95' => '38.25', 'แก๊สโซฮอล์_91' => '37.98', 'แก๊สโซฮอล์_E20' => '35.94', 'แก๊สโซฮอล์_E85' => '36.09', 'ดีเซลพรีเมี่ยม' => '41.54', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '31.10.2023', 'เบนซิน_95' => '46.34', 'แก๊สโซฮอล์_95' => '38.55', 'แก๊สโซฮอล์_91' => '38.28', 'แก๊สโซฮอล์_E20' => '36.24', 'แก๊สโซฮอล์_E85' => '36.39', 'ดีเซลพรีเมี่ยม' => '41.24', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '21.10.2023', 'เบนซิน_95' => '46.04', 'แก๊สโซฮอล์_95' => '38.25', 'แก๊สโซฮอล์_91' => '37.98', 'แก๊สโซฮอล์_E20' => '35.94', 'แก๊สโซฮอล์_E85' => '36.09', 'ดีเซลพรีเมี่ยม' => '41.24', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '17.10.2023', 'เบนซิน_95' => '45.64', 'แก๊สโซฮอล์_95' => '37.85', 'แก๊สโซฮอล์_91' => '37.58', 'แก๊สโซฮอล์_E20' => '35.54', 'แก๊สโซฮอล์_E85' => '35.69', 'ดีเซลพรีเมี่ยม' => '40.84', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '17.10.2023', 'เบนซิน_95' => '45.94', 'แก๊สโซฮอล์_95' => '38.15', 'แก๊สโซฮอล์_91' => '37.88', 'แก๊สโซฮอล์_E20' => '35.84', 'แก๊สโซฮอล์_E85' => '35.99', 'ดีเซลพรีเมี่ยม' => '40.84', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '07.10.2023', 'เบนซิน_95' => '45.54', 'แก๊สโซฮอล์_95' => '37.75', 'แก๊สโซฮอล์_91' => '37.48', 'แก๊สโซฮอล์_E20' => '35.44', 'แก๊สโซฮอล์_E85' => '35.59', 'ดีเซลพรีเมี่ยม' => '40.24', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
        ['วันที่' => '06.10.2023', 'เบนซิน_95' => '46.04', 'แก๊สโซฮอล์_95' => '38.25', 'แก๊สโซฮอล์_91' => '37.98', 'แก๊สโซฮอล์_E20' => '35.94', 'แก๊สโซฮอล์_E85' => '35.59', 'ดีเซลพรีเมี่ยม' => '40.24', 'ดีเซล_B20' => '29.94', 'ดีเซล' => '29.94', 'ดีเซล_B7' => '29.94', 'แก๊ส_NGV' => '17.59'],
    ];

  $Gasohol_95 = wp_get_attachment_image_url(32361, '95');
    $Gasoline_95 = wp_get_attachment_image_url(32361, 'Gasoline 95');
    $Gasohol_91 = wp_get_attachment_image_url(32361, 'Gasohol 91');
    $Gasohol_E20 = wp_get_attachment_image_url(32361, 'Gasohol E20');
    $Gasohol_E85 = wp_get_attachment_image_url(32361, 'Gasohol E85');
    $Diesel_B7 = wp_get_attachment_image_url(32361, 'Diesel B7');
	$Diesel = wp_get_attachment_image_url(32361, 'Diesel');
	$Diesel_B20 = wp_get_attachment_image_url(32361, 'Diesel B20');
	$Premium_Diesel = wp_get_attachment_image_url(32361, 'Premium Diesel');
    $NGV_gas = wp_get_attachment_image_url(32361, 'NGV gas');

    $petrol_colors = [
        'Xăng E5 RON 92-II' => [
            'color' => '#FFc000',
            'icon' =>  $Gasohol_95,
        ],
        'Xăng RON 95-III' => [
            'color' => '#FFc000',
            'icon' => $Gasoline_95,
        ],
        'แก๊สโซฮอล์ 91' => [
            'color' => '#FFc000',
            'icon' => $Gasohol_91,
        ],
        'แก๊สโซฮอล์ E20' => [
            'color' => '#FFc000',
            'icon' =>  $Gasohol_E20,
        ],
		 'แก๊สโซฮอล์ E85' => [
            'color' => '#FFc000',
            'icon' =>  $Gasohol_E85,
        ],
    ];

    // Define colors and icons for diesel variants
    $diesel_colors = [
        'Dầu DO 0,05S-II' => [
            'color' => '#FFc000',
            'icon' => $Diesel_B7,
        ],
        'ดีเซล' => [
            'color' => '#FFc000',
            'icon' => $Diesel,
        ],
		  'ดีเซล B20' => [
            'color' => '#FFc000',
            'icon' => $Diesel_B20,
        ],
        'ดีเซลพรีเมี่ยม' => [
            'color' => '#FFc000',
            'icon' => $Premium_Diesel,
        ],
    ];

    $gas_colors = [
        'Dầu KO' => [
            'color' => '#FFc000',
            'icon' => $NGV_gas,
        ],
    ];


    ob_start();

?>
    <div class="fuel-price-wrapper">
        <h2 class="wa-title-text">Giá Xăng Dầu Ngày Hôm Nay ở Việt Nam</h2>
        <div class="fuel-price-section">
            <?php foreach ($petrol_prices as $petrol) : ?>
                <div class="fuel-price-item">
                    <div class="fuel-info">
                        <img src="<?php echo $petrol_colors[$petrol['variant']]['icon'] ?? $petrol_colors['RON 95']['icon']; ?>"
                            alt="<?php echo esc_attr($petrol['variant']); ?>"
                            class="fuel-img">
                        <p class="fuel-type" style="color: <?php echo esc_attr($petrol_colors[$petrol['variant']]['color'] ?? '#FFD700'); ?>;">
                            <?php echo esc_html($petrol['variant']); ?>
                        </p>
                        <p class="fuel-price">
                            <?php echo esc_html(format_price_vietnam($petrol['price'])); ?>
                        </p>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>



        <h2 class="wa-title-text">Giá Dầu Diesel Ngày Hôm Nay ở Việt Nam</h2>
        <div class="fuel-price-section">
            <?php foreach ($diesel_prices as $diesel) : ?>
                <div class="fuel-price-item">
                    <div class="fuel-info">
                        <img src="<?php echo $diesel_colors[$diesel['variant']]['icon'] ?? $diesel_colors['EURO 5 B10']['icon']; ?>"
                            alt="<?php echo esc_attr($diesel['variant']); ?>"
                            class="fuel-img">
                        <p class="fuel-type" style="color: <?php echo esc_attr($diesel_colors[$diesel['variant']]['color'] ?? '#228B22'); ?>;">
                            <?php echo esc_html($diesel['variant']); ?>
                        </p>
                        <p class="fuel-price">
                            <?php echo esc_html(format_price_vietnam($diesel['price'])); ?>
                        </p>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        
        <h2 class="wa-title-text">Giá Dầu Hỏa Ngày Hôm Nay ở Việt Nam</h2>
        <div class="fuel-price-section">
            <?php foreach ( $gas_prices as $gas) : ?>
                <div class="fuel-price-item">
                    <div class="fuel-info">
                        <img src="<?php echo $gas_colors[$gas['variant']]['icon'] ?? $gas_colors['EURO 5 B10']['icon']; ?>"
                            alt="<?php echo esc_attr($gas['variant']); ?>"
                            class="fuel-img">
                        <p class="fuel-type" style="color: <?php echo esc_attr($gas_colors[$gas['variant']]['color'] ?? '#6232C5'); ?>;">
                            <?php echo esc_html($gas['variant']); ?>
                        </p>
                        <p class="fuel-price">
                           <?php echo esc_html(format_price_vietnam($gas['price'])); ?>
                        </p>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>

        <div class="fuel-note">
            <p>Giá xăng RON95, RON92 và dầu diesel tại Việt Nam sẽ được cập nhật tại trang này. Dự báo và dự đoán giá xăng mới nhất tuần sau sẽ được công bố vào ngày hôm trước (nếu có). Bảng trên là giá bán lẻ xăng RON95, RON92 và dầu diesel mới nhất do Tổng Công ty Dầu Việt Nam - CTCP công bố. Giá xăng dầu chính thức tại Việt Nam sẽ được công bố hàng tuần hoặc Tổng công ty Dầu Việt Nam - CTCP công bố. Giá xăng dầu trên trang này chỉ dùng tham khảo, không được coi là trang chính thức của bất kỳ cơ quan hoặc bên liên quan nào liên quan đến việc điều chỉnh giá xăng dầu. autofun.vn sẽ không chịu trách nhiệm với bạn về bất kỳ tổn thất hoặc thiệt hại nào gây ra cho bạn bởi nội dung không chính xác (nếu có) được công bố trên trang web này.</p>
        </div>
        <h2 class="list-title"><span>Bảng Lịch Sử Giá Xăng Dầu Tại Việt Nam</span></h2>
        <div class="scroll-bar" style="max-height: 519px; overflow-y: scroll; scrollbar-width: none; -ms-overflow-style: none;">
            <div class="scroll-bar" style="max-height: 519px; overflow-y: scroll;overflow-y: scroll; scrollbar-width: none; -ms-overflow-style: none;">
                <table style="border-collapse: collapse; width: 100%; border: 1px solid #ddd;">
                    <thead>
                        <tr>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0; text-align:center;">Giá nhiên liệu	</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">Xăng RON 95 1 lít	</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">Xăng E5 RON 92 1 lít	</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">Dầu Diesel 1 lít	</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">Dầu Hỏa 1 lít </th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">แก๊สโซฮอล์ E85</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">ดีเซลพรีเมี่ยม</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">ดีเซล B20</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">ดีเซล</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">ดีเซล B7</th>
                            <th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2; position: sticky; top: 0;text-align:center;">แก๊ส NGV</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($fuel_price_data as $data) : ?>
                            <tr>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['วันที่']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['เบนซิน_95']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['แก๊สโซฮอล์_95']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['แก๊สโซฮอล์_91']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['แก๊สโซฮอล์_E20']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['แก๊สโซฮอล์_E85']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['ดีเซลพรีเมี่ยม']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['ดีเซล_B20']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['ดีเซล']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['ดีเซล_B7']; ?></td>
                                <td style="border: 1px solid #ddd; padding: 8px;text-align:center;"><?php echo $data['แก๊ส_NGV']; ?></td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </div>


    </div>
    <style>
        .list-title {
            display: block;
            font-family: "Roboto Condensed";
            position: relative;
            font-size: 26px;
            line-height: 32px;
            padding: 8px 0;
            color: #262626;
        }

        .scroll-bar {
            max-height: 519px;
            overflow-y: scroll;
            scrollbar-width: none;
            -ms-overflow-style: none;
        }

        .scroll-bar::-webkit-scrollbar {
            display: none;
        }

        .fuel-price-wrapper {
            font-family: Arial, sans-serif;
            color: #333;
        }

        .fuel-info img {
            width: 61px;
            height: 82px;
            object-fit: contain;
            position: absolute;
            bottom: 0;
            left: 20px;
        }

        .fuel-price-wrapper h2 {
            font-size: 24px;
            margin-bottom: 15px;
        }

        .fuel-price-section {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 30px;
            margin-bottom: 30px;
            position: relative;

        }

        .fuel-price-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 10px;
            border: 1px solid #e0e0e0;
            border-radius: 3px;
            background-color: #ffffff;
            width: 100%;
            height: 82px;
            position: relative;
        }

        .fuel-icon {
            width: 40px;
            height: 40px;
            margin-right: 15px;
            background-size: cover;
        }

        .petrol-icon {
            background-image: url('path-to-your-petrol-icon.png');
        }

        .diesel-icon {
            background-image: url('path-to-your-diesel-icon.png');
        }

        .fuel-type {
            font-weight: bold;
            font-size: 21px;
            position: relative;
            top: 18px;
        }

        .fuel-price-group {
            text-align: right;
            display: flex;
            flex-direction: column;
            flex-grow: 1;
            /* Ensure the price group takes up available space */
            justify-content: center;
        }

        .fuel-price {
            font-size: 21px;
            font-weight: bold;
            margin-left: 15px;
            position: absolute;
            top: 26px;
            right: 10px;

        }

        .price-change {
            font-size: 14px;
            margin-left: 10px;
            margin-top: 5px;
            position: absolute;
            right: 18px;
            top: 5px;
        }

        .fuel-note {
            font-size: 12px;
            color: #666;
            line-height: 1.6;
            display: block;
            margin-block-start: 1em;
            margin-block-end: 1em;
            margin-inline-start: 0px;
            margin-inline-end: 0px;
            unicode-bidi: isolate;
        }
    </style>
<?php
    return ob_get_clean();
}

add_shortcode('fuel_price_template', 'fuel_price_template_shortcode');
?>