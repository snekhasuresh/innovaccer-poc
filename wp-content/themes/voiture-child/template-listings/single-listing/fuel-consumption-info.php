<?php

function fuel_consumption_info_shortcode()
{
    ob_start();
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    $global_listing_data = get_listing_from_query_vars();
    $car_post = $global_listing_data['post'];
    $variant_posts = $global_listing_data['variant_posts'];
    $all_variant_meta = $global_listing_data['variant_meta_data'];

    $table_data = array();
    $lowest_manufacturer_claim = 999999999;
    $highest_manufacturer_claim = 0;
    // Loop through all variant posts and collect their data
    if (!empty($variant_posts)) {
        foreach ($variant_posts as $v => $variant_post) {
            $variant_meta = $all_variant_meta[$variant_post->ID];
            $consumption = isset($variant_meta['manufacturers_claim'][0]) ? $variant_meta['manufacturers_claim'][0] : '';

            // split by " " and take the first word and convert to number
            if (!empty($consumption)) {
                $consumption = floatval(explode(" ", $consumption)[0]);

                if ($consumption < $lowest_manufacturer_claim) {
                    $lowest_manufacturer_claim = $consumption;
                }

                if ($consumption > $highest_manufacturer_claim) {
                    $highest_manufacturer_claim = $consumption;
                }
            }


            // Add each variant's data to the table_data array
            if (!empty($consumption)) {
                $table_data[$variant_post->post_title][] = [
                    'consumption' => $consumption
                ];
            }
        }

        if ($lowest_manufacturer_claim == 999999999) {
            $lowest_manufacturer_claim = '--';
        }

        if ($highest_manufacturer_claim == 0) {
            $highest_manufacturer_claim = '--';
        }
    }

    echo '<h1 class="wa-title-text"> Mức Tiêu Hao Nhiên Liệu Của Xe ' . $car_post->post_title . ' </h1>';

    echo '<div class="fuel-consumption-container">';
    echo "<p>Mức tiêu hao nhiên liệu của xe " . $car_post->post_title . "  là  " . $lowest_manufacturer_claim . "  (tiết kiệm nhiên liệu nhất), và mức tiêu hao nhiên liệu cao nhất là  " . $highest_manufacturer_claim . ".</p>";
    echo "<p> Định mức tiêu hao nhiên liệu là thông số kỹ thuật được các nhà sản xuất xe đưa ra qua việc tính toán mức nhiên liệu mà xe sẽ tiêu thụ khi chạy trên quãng đường nhất định (100km), trong điều kiện tiêu chuẩn, ký hiệu L / 100 km. </p>";

    echo "<div id='fuel-consumption-content'>";
  echo "<p>Dưới đây là mức tiêu thụ nhiên liệu của xe " . $car_post->post_title . ", theo thông tin chính thức từ " . ucfirst($make) . ":</p>";


   foreach ($table_data as $variant => $data) {
    echo '<p>Mức tiêu thụ nhiên liệu của ' . $variant . ' là ' . $data[0]['consumption'] . '.</p>';
}

echo "<p>Hiệu suất nhiên liệu, hay còn gọi là mức tiêu hao nhiên liệu nghịch đảo, là một chỉ số phổ biến khác, được tính bằng quãng đường di chuyển trên một đơn vị nhiên liệu, chẳng hạn như km/lít hoặc dặm/gallon.</p>";
echo "<p>Mức tiêu thụ nhiên liệu của một chiếc xe phụ thuộc chủ yếu vào công nghệ động cơ và kích thước của nó. Các yếu tố ảnh hưởng bao gồm:</p>";
echo "<ul>
        <li>1. Điều kiện đường xá, giao thông và thời tiết</li>
        <li>2. Phong cách lái xe</li>
        <li>3. Tốc độ, tải trọng và tình trạng xe</li>
      </ul>";
echo "<p>Công thức sau được sử dụng để tính mức tiêu thụ nhiên liệu theo lít/100km, đây là cách đo phổ biến nhất:</p>";
echo "<p>(Lít nhiên liệu đã sử dụng × 100) ÷ số km đã đi = lít trên 100 km.</p>";
echo "<p>Bằng cách này, bạn có thể dễ dàng tính chi phí nhiên liệu hàng tháng của " . $car_post->post_title . " bằng cách sử dụng công cụ tính chi phí nhiên liệu của chúng tôi.</p>";
echo "</div>";

echo '<span id="toggle-button" onclick="toggleContent()">Đọc thêm</span>';
echo '</div>';


?>
    <script>
        function toggleContent() {
            var content = document.getElementById("fuel-consumption-content");
            var button = document.getElementById("toggle-button");

            if (content.style.display === "none" || content.style.display === "") {
                content.style.display = "block";
                button.innerText = "Ẩn";
            } else {
                content.style.display = "none";
                button.innerText = "Đọc thêm";
            }
        }
    </script>
    <style>
         .fuel-consumption-container {
            background-color: #f0f0f0;
            /* Light grey background */
            padding: 20px;
            border-radius: 8px;
            position: relative;
        }
		.fuel-consumption-container p{
			color:#262626;
			font-family:"Roboto";
			font-size:14px;
		}
        .fuel-consumption-table {
            font-family: 'Roboto' !important;
            color: #262626 !important;
        }

        #fuel-consumption-content {
            display: none;
            margin-bottom: 20px;
        }

        #toggle-button {
            font-weight: bold;
            color: #576b97;
			font-family:"Roboto";
			font-size:16px;
            /* Brinjal color */
            cursor: pointer;
            position: absolute;
            bottom: 10px;
            right: 10px;
        }

        #toggle-button:hover {
            text-decoration: underline;
        }
    </style>
<?php

    return ob_get_clean();
}

add_shortcode('fuel_consumption_info', 'fuel_consumption_info_shortcode');
