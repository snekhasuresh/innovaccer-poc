<?php

function motor_fuel_consumption_info_shortcode()
{
    ob_start();
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    $global_listing_data = get_motor_listing_from_query_vars();
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
            $consumption = isset($variant_meta['fuel_consumption'][0]) ? $variant_meta['fuel_consumption'][0] : '';

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

    echo '<h1> อัตราสิ้นเปลืองเชื้อเพลิง ' . $car_post->post_title . '</h1>';

   echo '<div class="fuel-consumption-container">';
	echo "<p>ประสิทธิภาพด้านเชื้อเพลิงของ " . $car_post->post_title . " ตามที่ข้อมูลทางการของ " . ucfirst($make) . " มีดังนี้:</p>";
 	echo "<div id='fuel-consumption-content'>";
	foreach ($table_data as $variant => $data) {
		echo '<p>อัตราสิ้นเปลืองเชื้อเพลิงของ ' . $variant . ' คือ ' . ($data[0]['consumption'] ?: '-') . ' L/100km.</p>';
	}

	echo "<p>ขี่มอเตอร์ไซค์อย่างไรให้ประหยัดน้ำมันที่สุด?</p>";
	echo "<ul>
			<li>1. รักษาสภาพลมยางอยู่เสมอ</li>
			<li>2. บำรุงรักษามอเตอร์ไซค์ของคุณอยู่เสมอ</li>
			<li>3. ขับด้วยความเร็วคงที่</li>
			<li>4. ใช้น้ำมันเชื้อเพลิงที่เหมาะสม</li>
			<li>5. ลดน้ำหนักบรรทุกของมอเตอร์ไซค์</li>
			<li>6. เป็นอากาศพลศาสตร์</li>
			<li>7. ขับบนไฮเวย์</li>
			<li>8. เปลี่ยนไส้กรองน้ำมันเชื้อเพลิงและอากาศ</li>
			<li>9. ตรวจสอบสภาพหัวเทียน คาร์บ และไอพ่น</li>
			<li>10. ทำความสะอาดเครื่องยนต์</li>
		  </ul>";
	echo '</div>';
	echo '<span id="toggle-button" onclick="toggleContent()">อ่านเพิ่มเติม</span>';
	echo '</div>';

?>
    <script>
        function toggleContent() {
            var content = document.getElementById("fuel-consumption-content");
            var button = document.getElementById("toggle-button");

            if (content.style.display === "none" || content.style.display === "") {
                content.style.display = "block";
                button.innerText = "ซ่อน";
            } else {
                content.style.display = "none";
                button.innerText = "อ่านเพิ่มเติม";
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
            color: #5D3A3A;
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

add_shortcode('motor_fuel_consumption_info', 'motor_fuel_consumption_info_shortcode');
