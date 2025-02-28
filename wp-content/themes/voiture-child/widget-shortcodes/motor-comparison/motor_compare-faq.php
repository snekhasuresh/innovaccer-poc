<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';
function enqueue_motor_compare_faq_css()
{
    wp_enqueue_style('motor-compare-faq-shortcode-css', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-comparison/css/motor-compare-faq.css');
}
function compare_motor_faqs_shortcode($atts)
{

    // Static FAQ data array
    $static_faqs = array(
        array(
            'question' => 'เลือกซื้อรถยนต์อย่างไรให้เหมาะกับตัวเอง?',
            'answer' => 'ด้วยเครื่องมือเปรียบเทียบรถยนต์ หลังจากที่คุณมีความเข้าใจโดยละเอียดเกี่ยวกับข้อมูลของรุ่นที่เปรียบเทียบกันแล้ว คุณสามารถตัดสินใจเลือกรถยนต์ที่เหมาะกับตัวเองได้'
        ),
        array(
            'question' => 'การเปรียบเทียบประสิทธิภาพของรถยนต์มีอะไรบ้าง?',
            'answer' => 'การเปรียบเทียบประสิทธิภาพส่วนใหญ่รวมถึงการเร่งความเร็ว การเบรก การควบคุมรถ และด้านออฟโรด'
        ),
        array(
            'question' => 'เปรียบเทียบประกันรถยนต์อย่างไร?',
            'answer' => 'คุณต้องขอใบเสนอราคาและเปรียบเทียบแผนบริการจากผู้ให้บริการหลายรายเพื่อได้รับราคาที่คุ้มค่าที่สุด'
        ),
        array(
            'question' => 'สามารถเปรียบเทียบราคารถยนต์ได้ที่ไหน?',
            'answer' => 'คุณสามารถใช้เครื่องมือเปรียบเทียบรถได้ที่ AutoFun เพื่อเปรียบเทียบราคารถ และรับข้อมูลการเปรียบเทียบราคา'
        ),
        array(
            'question' => 'ถคันไหนประหยัดน้ำมันที่สุด?',
            'answer' => 'การเปรียบเทียบการบริโภคน้ำมันเชื้อเพลิงของรถยนต์ เราขอแนะนำรถที่ประหยัดน้ำมันที่สุดในประเทศไทย เพียงแค่เปรียบเทียบมากกว่ารถเพื่อให้ได้รับประหยัดน้ำมันของพวกเขาจะรู้คำตอบ.'
        ),
    );

    add_faq_json_ld($static_faqs);

    ob_start();
?>
    <div id="listing-detail-description" class="description inner compare-faq">
        <h2 class="wa-title-text">คำถามที่พบบ่อยที่เกี่ยวข้อง</h2>
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <div class="acc">

                        <?php
                        // Display static FAQ content
                        foreach ($static_faqs as $index => $faq) {
                            echo '<div class="accordion-item">';
                            echo '<input type="checkbox" id="static-faq-' . $index . '">';
                            echo '<label for="static-faq-' . $index . '" class="accordion-header">' . esc_html($faq['question']) . '</label>';
                            echo '<div class="accordion-content">' . wp_kses_post($faq['answer']) . '</div>';
                            echo '</div>';
                        }

                        ?>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <style>

    </style>
<?php

    $output = ob_get_clean();
    return $output;
}
add_shortcode('compare_motor_faqs_shortcode', 'compare_motor_faqs_shortcode');
