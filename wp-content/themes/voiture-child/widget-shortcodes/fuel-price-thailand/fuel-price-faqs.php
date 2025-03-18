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
            'question' => 'Xăng 92 và 95 là gì?',
            'answer'   => 'Các con số 92, 95 là chỉ số octan - chỉ số biểu thị khả năng chống kích nổ của xăng. Theo đó, chỉ số octan càng cao đồng nghĩa với khả năng chống kích nổ càng cao. Xăng là một loại chất lỏng dễ cháy có nguồn gốc từ dầu mỏ, được sử dụng làm nhiên liệu cho hầu hết các loại động cơ đốt trong. Hiện nay, cùng với xăng sinh học E5, xăng 92 và 95 là 3 loại xăng phổ biến trên thị trường.'
        ),
        array(
            'question' => 'Dầu diesel là gì?',
            'answer'   => 'Dầu diesel còn được biết đến với tên gọi khác là dầu gazole (DO), đặc điểm dầu diesel là một loại nhiên liệu lỏng, được tinh chế từ dầu mỏ có thành phần chưng cất nằm giữa dầu hỏa và dầu bôi trơn công nghiệp. Chúng thường có trọng lượng nặng hơn xăng và dầu lửa, nhiệt độ bốc hơi từ 175 - 370 độ C. Hiện nay, các phương tiện tại Việt Nam đang sử dụng 2 loại dầu diesel phổ biến là DO 0,25%S và DO 0,005%S. Dầu diesel được biết đến như là một loại nhiên liệu đa năng, khi có thể sử dụng cho phần lớn các loại phương tiện giao thông hiện nay, từ đường bộ, đường thủy.'
        ),
        array(
            'question' => 'Dầu diesel hay xăng tốt hơn?',
            'answer'   => 'So với động cơ xăng thì động cơ dầu/diesel có hiệu suất cao hơn 1,5 lần mà giá thành lại rẻ hơn. Bên cạnh đó, động cơ dầu diesel an toàn hơn vì dầu không bốc cháy ở nhiệt độ thường nên ít gây nguy hiểm do hỏa hoạn so với động cơ xăng.'
        )
    );

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering

    if (!empty($faqs)) {
?>

        <div id="fuel-listing-detail-description" class="fuel-description inner">
            <h2 class="wa-title-text">Câu Hỏi Thường Gặp về Giá Xăng Dầu</h2>
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
