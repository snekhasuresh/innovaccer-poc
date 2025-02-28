<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function fuel_cost_data($atts)
{
    $fuel_data = [
        ['label' => 'Car Model', 'value' => 'Honda City 1.5V Sensing'],
        ['label' => 'Fuel Consumption', 'value' => '5.2 L/100km'],
        ['label' => 'Mileage', 'value' => '2000 KM/Year'],
        ['label' => 'Fuel Type', 'value' => 'RON 95'],
        ['label' => 'Fuel Price (RM)', 'value' => 'RM 2.05'],
        ['label' => 'Yearly Fuel Payment', 'value' => 'RM 213.2'],
    ];

?>
    <h2 class="wa-title-text">An Example of the Petrol Cost Calculator</h2>
    <div style="border: 1px solid #dddddd; border-collapse: collapse; margin-top: 20px;">
        <?php foreach ($fuel_data as $data): ?>
            <div style="display: flex; border-bottom: 1px solid #dddddd;">
                <div style="padding: 10px; flex: 1; font-weight: bold; border-right: 1px solid #dddddd;">
                    <?php echo $data['label']; ?>
                </div>
                <div style="padding: 10px; flex: 1;">
                    <?php echo $data['value']; ?>
                </div>
            </div>
        <?php endforeach; ?>
    </div>
<?php
}
add_shortcode('fuel_cost_data', 'fuel_cost_data');
?>

<?php
function fuel_cost_faqs()
{
    // FAQ data
    $faqs = [
        [
            'question' => 'ราคาน้ำมันเมืองไทยวันนี้เท่าไหร่',
            'answer' => 'ราคาน้ำมันล่าสุดในประเทศไทยวันนี้ ท่านสามารถคลิกที่นี่เพื่อดู: ราคาน้ำมันวันนี้'
        ],
        [
            'question' => 'วิธีการคำนวณค่าน้ำมันเชื้อเพลิง',
            'answer' => 'การคิดค่าน้ำมัน ให้นำ จำนวนน้ำมันเชื้อเพลิง(ลิตร) คูณ ราคาน้ำมัน ณ ปัจจุบัน. รถตู้ไปสะเดา ระยะทางไป-กลับ=140 ก.ม (รถตู้ 7 ก.ม./ลิตร), จำนวนลิตร = 20 ลิตร (มาจาก 140/7 = 20), สมมติให้ราคาน้ำมัน ณ ปัจจุบัน คือ 37.85 บาท. คิดค่าน้ำัมัน = จำนวนน้ำมันเชื้อเพลิง(ลิตร) x ราคาน้ำมัน = 20 x 37.85 = 757 บาท, ดังนั้น ค่าน้ำมันเชื้อเพลิงรถตู้ไป-กลับสะเดาประมาณ 757 บาท'
        ],
        [
            'question' => 'วิธีขับรถให้ประหยัดน้ำมัน',
            'answer' => '1.เช็คสภาพรถทุกอย่างก่อนและหลังใช้งานเสมอ 2.ใส่ใจเรื่องเครื่องยนต์ให้มากขึ้น 3.อย่าบรรทุกของหนักเยอะจนเกินไป 4.การเปิดแอร์ที่เย็นจัด 5.ไม่ควรเร่งเครื่องขณะที่จอดรถนิ่งอยู่ 6.ออกรถช้า ๆ ไม่ต้องรีบ ใครที่ชอบขับรถซิ่งหรือออกตัวรถแรง ขอบอกเลยว่าการออกตัวรถแบบนั้นจะมเป็นการเพิ่มอัตราสิ้นเปลืองน้ำมันโดยเปล่าประโยชน์ เนื่องจากการออกตัวแรงทำให้เครื่องยนต์ต้องคุณต้องสูบน้ำมันออกมาเป็นจำนวนมากเพื่อให้รถพุ่งตัวไปข้างหน้าอย่างรวดเร็วนั่นเอง รวมถึงในขณะขับรถอยู่ คุณควรใช้ความเร็วที่เสมอตลอดทาง ไม่ควรเบรกโดยไม่จำเป็น เพราะการเบรกบ่อย ๆ เป็นอีกหนึ่งต้นเหตุที่ทำให้เปลืองน้ำมัน'
        ],
    ];

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">Fuel Cost Calculator In Malaysia FAQs</h2>

    <div class="fuel-cost-faq-container">
        <?php foreach ($faqs as $faq): ?>
            <div class="fuel-cost-faq-item">
                <div class="fuel-cost-faq-question">
                    <?php echo esc_html($faq['question']); ?>
                    <!-- <span class="arrow"><i class="fas fa-chevron-down"></i></span> -->
                </div>
                <div class="fule-cost-arrow-container">
                    <span class="arrow"><i class="fas fa-chevron-down"></i></span> <!-- Font Awesome down arrow -->
                </div>
                <div class="fule-cost-faq-answer"><?php echo esc_html($faq['answer']); ?></div>
                <hr /> <!-- Line separator between FAQs -->
            </div>
        <?php endforeach; ?>
    </div>

    <style>
        .fuel-cost-faq-container {
            /* max-width: 74%; */
            margin-top: 20px;
            border: 1px solid #ddd;
            /* Outer border for the entire container */
            border-radius: 5px;
            overflow: hidden;
            /* Ensure border radius works */
        }

        .fuel-cost-faq-item {

            cursor: pointer;
            position: relative;
            /* Make it a positioned element */
        }

        .fuel-cost-faq-item:last-child hr {
            display: none;
            /* Hide the last line separator */
        }

        .fuel-cost-faq-question {
            font-family: 'Roboto';
            padding: 19px 44px 19px 16px;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            font-weight: bold;
            position: relative;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .fule-cost-faq-answer {
            overflow: hidden;
            padding: 0 44px 16px 16px;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            display: none;
            transition: all .2s;
        }

        .fuel-cost-faq-item:hover {
            background-color: #f5f5f5;
            /* Optional hover effect */
        }

        .fule-cost-arrow-container {
            position: absolute;
            /* Position the arrow absolutely */
            right: 15px;
            /* Align to the right with a margin */
            top: 50%;
            /* Center vertically */
            transform: translateY(-50%);
            /* Adjust to center */
            transition: transform 0.2s ease;
            /* Transition for rotation */
        }

        .fuel-cost-faq-item.active .fule-cost-arrow-container {
            transform: translateY(-50%) rotate(180deg);
            /* Rotate arrow when active */
        }
    </style>

    <script>
        document.querySelectorAll('.fuel-cost-faq-item').forEach(item => {
            item.addEventListener('click', () => {
                const answer = item.querySelector('.fule-cost-faq-answer');
                answer.style.display = answer.style.display === 'none' || answer.style.display === '' ? 'block' : 'none';

                // Toggle active class for arrow rotation
                item.classList.toggle('active');
            });
        });
    </script>


<?php
    return ob_get_clean(); // Return the buffered output
}
add_shortcode('fuel_cost_faqs', 'fuel_cost_faqs');
?>