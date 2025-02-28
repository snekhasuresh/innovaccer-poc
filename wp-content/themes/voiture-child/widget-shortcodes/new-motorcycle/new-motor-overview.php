<?php
function newbike_overview($atts)
{
    // Extract attributes from the shortcode
    $atts = shortcode_atts(array(
        'image_url' => esc_url(get_site_url() . '/wp-content/uploads/2024/10/display_ev_top_banner_for_newcars.jpg'),  // Image URL
        'link_url' => '',   // Link URL
        'alt_text' => '',   // Alt text for the image
    ), $atts);

    // Validate the image URL
    if (empty($atts['image_url'])) {
        return ''; // Return empty if no image URL is provided
    }

    ob_start();
?>
    <div class="new-cars-overview">
        <span class="overview-text">
            ค้นหาข้อมูลรถมอเตอร์ไซค์ใหม่ทั้งหมดในประเทศไทยในปี 2024 ได้ที่ AutoFun Thailand Motorcycles รวมทั้งราคารถจักรยานยนต์ล่าสุด ราคาบิ๊กไบค์ ราคารถมอเตอร์ไซค์ไฟฟ้า สเปครถจักรยานยนต์ รูปภาพ คุณสมบัติ ข่าว รีวิวโดยผู้เชี่ยวชาญ โปรโมชั่น สินเชื่อรถจักรยานยนต์ คู่มือการซื้อรถยนต์ และอื่นๆ ครอบคลุม Honda, Yamaha, GPX, Kawasaki, SUzuki, Aprilia, Bajaj, Benelli, Ducati, BMW, KYM, Vespa และรถมอเตอร์ไซค์แบรนด์ดังอื่นๆ'
            <span class="hidden-content">
                รุ่นรถจักรยานยนต์ที่จำหน่ายในประเทศไทย ได้แก่ GPX Drone, Yamaha Aerox, Yamaha Finn, Yamaha Nmax, GPX Demon 150GR, Honda PCX, Honda Wave 110i, Yamaha Grand Filano, Honda Monkey, Yamaha XSR155, Honda Adv 150, Honda Super Cub, GPX Legend 200, Honda Scoopy i, GPX Legend 250 Twin, Ducati Scrambler, Honda Click 150i, Yamaha QBIX, Honda Rebel 300, Honda Click 125i, Kawasaki W175, KTM Duke 390, Vespa Sprint 150 I-GET, Ducati Panigale, Vespa Sprint 125 I-GET, Vespa Primavera 150 I-GET.
                ยินดีต้อนรับสู่AutoFun! เรามีวิธีการที่หลากหลายเพื่อช่วยให้คุณตัดสินใจเลือกรถมอเตอร์ไซค์ที่คุณต้องการ คุณสามารถดูสเปกจักรยานแบบเต็ม คำวิจารณ์จากผู้เชี่ยวชาญ รูปภาพ วิดีโอ มุมมอง 360 องศา และคำวิจารณ์การทดสอบถนนจากผู้เชี่ยวชาญของเรา นอกจากนี้เรายังมีโปรโมชั่นสินเชื่อรถจักรยานยนต์และประกันภัยมอเตอร์ไซค์ที่น่าสนใจอีกมากมาย เราหวังว่าคุณจะพบรถมอเตอร์ไซค์ในฝันของคุณในประเทศไทย!
            </span>
        </span>
        <a class="toggle-content-btn">อ่านเพิ่มเติม</a>
    </div>
    <style>
        .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:30px;
        }

        .hidden-content {
            display: none;
        }

        .toggle-content-btn {
            margin-top: 10px;
            color: #576b95;
            border: none;
            padding: 5px 10px;
            cursor: pointer;
            border-radius: 3px;
			font-weight:700;
			font-size:16px;
			font-family:'Roboto';
        }

        .toggle-content-btn:hover {
           color: #576b95;
        }
			@media screen and (max-width: 768px) {
			   .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:0px;
        }
		}
    </style>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const toggleButton = document.querySelector('.toggle-content-btn');
            const hiddenContent = document.querySelector('.hidden-content');

            if (toggleButton && hiddenContent) {
                toggleButton.addEventListener('click', function() {
                    if (hiddenContent.style.display === 'none' || hiddenContent.style.display === '') {
                        hiddenContent.style.display = 'inline';
                        toggleButton.textContent = 'ซ่อน';
                    } else {
                        hiddenContent.style.display = 'none';
                        toggleButton.textContent = 'อ่านเพิ่มเติม';
                    }
                });
            }
        });
    </script>
<?php
    return ob_get_clean();
}

add_shortcode('newbike_overview', 'newbike_overview');
