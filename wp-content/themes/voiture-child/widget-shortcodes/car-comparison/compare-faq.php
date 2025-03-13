<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function compare_car_faqs_shortcode($atts)
{
    // Static FAQ data array
    $static_faqs = array(
        array(
            'question' => 'So sánh những gì trên xe?',
            'answer' => 'So sánh mã lực, tiêu hao nhiên liệu, công nghệ an toàn, phí dịch vụ vận chuyển và độ bền, đánh giá chấm điểm cho dễ so sánh'
        ),
        array(
            'question' => 'Vì sao phải so sánh xe này với xe kia?',
            'answer' => 'Vì khi so sánh, bạn biết được nên mua xe nào trong cùng phân khúc, xe nào bền hơn, xe nào hợp với bạn hơn'
        ),
        array(
            'question' => 'Xe tốt nhất dành cho người mới lái tại Việt Nam?',
            'answer' => 'Toyota Vios là chiếc xe có đầy đủ tính năng mà người mới lái cần, nó là một chiếc xe phù hợp để trở thành chiếc xe đầu đời của bạn'
        ),
        array(
            'question' => 'So sánh những mẫu SUV tại Việt Nam, xe nào tốt nhất?',
            'answer' => 'Những chiếc SUV tốt nhất Việt Nam hiện tại gồm KIA Seltos, Mazda CX-5, Honda HR-V, Ford Ecosport, Hyundai Kona, Honda CR-V, Mitsubishi Outlander, Hyundai Tucson,,... Bạn có thể dễ dàng so sánh chúng thông qua công cụ hỗ trợ của Autofun'
        ),
        array(
            'question' => 'Sedan nào tốt nhất cho khách hàng gia đình?',
            'answer' => 'Điều này còn tùy thuộc vào ngân sách. Ở hạng B có Toyota Vios, Honda City, Mazda 2; C có Honda Civic, Chevrolet Cruze, Mazda 3, Hyundai Ioniq. Hãy tìm ra chiếc xe phù hợp nhất với mình thông qua công cụ so sánh xe'
        ),
    );

    add_faq_json_ld($static_faqs);

    ob_start();
?>
    <div id="listing-detail-description" class="description inner compare-faq">
        <h2 class="wa-title-text">Compare Cars FAQs</h2>
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <?php foreach ($static_faqs as $index => $faq): ?>
                        <div class="accordion-item">
                            <input type="checkbox" id="static-faq-<?php echo $index; ?>">
                            <label for="static-faq-<?php echo $index; ?>" class="accordion-header">
                                <?php echo esc_html($faq['question']); ?>
                            </label>
                            <div class="accordion-content">
                                <?php echo wp_kses_post($faq['answer']); ?>
                            </div>
                        </div>
                    <?php endforeach; ?>
                </div>
            </div>
        </div>
    </div>
    <style>
      

        .accordion {
            max-width: 100%;
            font-family: "Roboto";
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            border-left: 1px solid #e0e0e0;
            border-right: 1px solid #e0e0e0;
            border-top: 1px solid #e0e0e0;
        }

        .find-new-faq-con {
            margin-left: 30px;
        }

        .accordion-item {
            background: white;
            border-bottom: 1px solid #e0e0e0;
            position: relative;
        }

        .accordion-header {
            padding: 12px 15px;
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
            transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            font-size: 24px;
            font-weight: bold;
        }

        .accordion-content {
            background: white;
            overflow: hidden;
            max-height: 0;
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
            padding: 12px 15px;
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

        @media screen and (max-width: 768px) {
            .accordion-header::after {
                content: "›";
                position: absolute;
                top: 50%;
                transform: translateY(-50%) rotate(90deg);
                transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                font-size: 24px;
                font-weight: bold;
            }

            .find-new-faq-con {
                margin-left: 0px !important;
            }
        }
    </style>
<?php

    $output = ob_get_clean();
    return $output;
}
add_shortcode('compare_car_faqs_shortcode', 'compare_car_faqs_shortcode');
