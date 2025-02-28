<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function fuel_car_faqs_shortcode($atts)
{
    $atts = shortcode_atts(array(
        'brand_id' => '',
    ), $atts);

    // Static array of FAQs (replace this array with your actual questions and answers)
    $faqs = array(
        array(
            'question' => 'ราคาน้ำมันขายปลีกประกอบไปด้วยปัจจัยอะไรบ้าง',
            'answer'   => 'ราคาน้ำมัน ณ สถานีบริการน้ำมันประกอบด้วยต้นทุนการผลิต ภาษีสรรพสามิต ภาษีอื่นๆ และเงินสมทบกองทุนของรัฐบาล และค่าการตลาด (ค่าใช้จ่ายในการขาย ดำเนินการ การขนส่ง กำไร และอื่นๆ)'
        ),
        array(
            'question' => 'เหตุใดราคาน้ำมันเบนซินจึงแพงกว่าราคาน้ำมันดีเซล',
            'answer'   => 'ราคาน้ำมันดีเซลมีราคาถูกกว่าราคาน้ำมันเบนซินเพราะอัตราภาษีสรรพสามิตสำหรับน้ำมันดีเซลต่ำกว่า'
        ),
        array(
            'question' => 'เหตุใดราคาน้ำมันจึงเพิ่มสูงขึ้นเมื่อเทียบกับปีก่อนๆ',
            'answer'   => 'ราคาน้ำมันดิบและน้ำมันสำเร็จรูปมีการปรับตัวสูงขึ้น จึงมีผลทำให้ราคาน้ำมันเชื้อเพลิงหน้าปั๊มเพิ่มสูงขึ้นตามกัน ทั้งนี้ราคาน้ำมันดิบและราคาน้ำมันสำเร็จรูปจะปรับตัวไปตามอิทธิพลของปัจจัยต่างๆ เช่น ความต้องการใช้น้ำมันที่เพิ่มขึ้น อุปสงค์การใช้น้ำมัน ขีดความสามารถและกำลังในการกลั่น ความต้องการน้ำมันตามฤดูกาล ภาวะการเมืองระหว่างประเทศ และสภาพอากาศที่รุนแรงหรือภัยธรรมชาติต่างๆ ปัจจัยเหล่านี้ส่งผลกระทบต่อปริมาณผลผลิตจากโรงกลั่น หรืออุปทานของน้ำมันเชื้อเพลิงในตลาดนั่นเอง'
        )
    );

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering

    if (!empty($faqs)) {
?>

        <div id="fuel-listing-detail-description" class="fuel-description inner">
            <h2 class="wa-title-text">คำถามที่พบบ่อยราคาน้ำมัน</h2>
            <div class="fuel-description-inner">
                <div class="fuel-description-inner-wrapper">
                    <div class="accordion">
                        <div class="acc">
                            <?php
                            foreach ($faqs as $index => $faq) {
                                // Use the static question and answer from the array
                                $question = esc_html($faq['question']);
                                $answer = wp_kses_post($faq['answer']);
                                $post_id = 'static-' . $index;  // Generate a static ID for each question

                                echo '<div class="accordion-item">';
                                echo '<input type="checkbox" id="faq-question-' . $post_id . '">';
                                echo '<label for="faq-question-' . $post_id . '" class="accordion-header">' . $question . '</label>';

                                echo '<div class="accordion-content">' . $answer . '</div>';
                                echo '</div>';
                            }
                            ?>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <style>
            .fuel-faq-container h2 {
                text-align: center;
                margin-bottom: 20px;
            }

            .acc {
                padding-left: 30px;
                padding-right: 30px;
            }

            .accordion {
                max-width: 100%;
                font-family: "Roboto";
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                border-left: 1px solid #e0e0e0;
                border-right: 1px solid #e0e0e0;
                border-top: 1px solid #e0e0e0;
            }

            .accordion-item {
                background: white;
                border-bottom: 1px solid #e0e0e0;
                position: relative;
            }

            .accordion-header {
                padding: 12px 0px;
                cursor: pointer;
                color: #262626;
                position: relative;
                user-select: none;
                font-weight: 700;
                display: block;
                background: white;
                font-size: 16px;
            }

            .accordion-header::after {
                content: "›";
                position: absolute;
                right: 20px;
                top: 50%;
                transform: translateY(-50%) rotate(90deg);
                /* transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1); */
                font-size: 24px;
                font-weight: bold;
            }

            .accordion-content {
                background: white;
                overflow: hidden;
                max-height: 0;
                /* transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); */
                padding: 0 20px;
                color: #666;
                opacity: 0;
            }

            .accordion-item input[type="checkbox"] {
                display: none;
            }

            .accordion-item {
                z-index: 1;
            }

            .accordion-item input[type="checkbox"]:checked {
                z-index: 2;
            }

            .accordion-item input[type="checkbox"]:checked+.accordion-header {
                background: white;
                z-index: 2;
                font-size: 16px;
                color: #262626;
                font-weight: 700;
                margin-bottom: -15px;
            }

            .accordion-item input[type="checkbox"]:checked+.accordion-header::after {
                transform: translateY(-50%) rotate(-90deg);
            }

            .accordion-item input[type="checkbox"]:checked~.accordion-content {
                max-height: 300px;
                opacity: 1;
                padding: 12px 00px;
                z-index: 2;
                font-size: 14px;
                color: #262626;
                font-weight: 400;
            }

            .accordion-content a {
                color: #576b95;
                font-weight: 700;
            }

            .accordion-item input[type="checkbox"]:checked~* {
                position: relative;
                z-index: 2;
            }
        </style>

<?php
    } else {
        echo '<p>' . esc_html__('No FAQs available.', 'voiture') . '</p>';
    }

    $output = ob_get_clean(); // Get the buffered content

    return $output; // Return the content for the shortcode
}
add_shortcode('fuel_car_faqs_shortcode', 'fuel_car_faqs_shortcode');
