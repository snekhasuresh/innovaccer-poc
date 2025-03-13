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
            'question' => 'Vì sao cần phải chọn loại xe trước khi mua?',
            'answer' => 'Vì trước khi mua xe máy, chúng ta cần so sánh chúng. Như trong cùng một tầm tiền, chiếc xe nào sẽ tốt hơn. Vì thế, công cụ so sánh của Autofun sẽ giúp bạn nhanh chóng tìm ra mẫu xe thích hợp với mình nhất.'
        ),
        array(
            'question' => 'Cỡ xe nào hợp với tôi nhất?',
            'answer' => 'Đầu tiên, bạn cần biết bạn thích cỡ xe nào, thứ hai, điều này còn tùy thuộc vào ngân sách của bạn. Bạn hoàn toàn có thể sử dụng công cụ so sánh này để tìm ra chiếc xe đúng cỡ và vừa túi tiền nhất!'
        ),
        array(
            'question' => 'Xe máy nào an toàn nhất?',
            'answer' => 'Bạn có thể so sánh mức độ an toàn của hai mẫu xe máy để xem đâu là mẫu xe an toàn nhất'
        ),
        array(
            'question' => 'Xe máy nào tốt nhất, giá nó bao nhiêu?',
            'answer' => 'Đầu tiên, những chiếc xe máy tốt nhất trên thị trường thường không rẻ. Thứ hai, chọn mẫu xe tốt trong tầm tiền của bạn sẽ dễ dàng hơn và công cụ so sánh ở đây để giúp bạn làm điều đó.'
        ),
        array(
            'question' => 'Xe tay ga Honda hay Yamaha tốt hơn?',
            'answer' => 'Xe Yamaha có amaha MIO Z, Yamaha Mio M3 125, Yamaha Mio S, Yamaha XRide 125, Yamaha FreeGo, Yamaha TMAX DX, Yamaha Janus, Yamaha Nozza Grande, Yamaha NVX. Honda có Honda Beat, Honda Genio, Honda Scoopy, Honda Vario 125, Honda Vario 150, Honda ADV 150, Honda Sh150i, Honda X-ADV, Honda PCX160, Honda PCX eHEV, Honda Winner X, Honda LEAD 125, Honda SH Mode 125, Honda Blade 110, Honda Wave RSX FI 110, Honda Vision, Honda Air Blade, Honda SH350i, Honda Future 125 FI. Bạn có thể dùng công cụ so sánh nhiều xe cùng lúc để tìm ra xe tốt nhất!'
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
