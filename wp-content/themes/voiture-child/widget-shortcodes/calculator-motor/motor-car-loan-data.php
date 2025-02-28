<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function motor_payment_faqs()
{
    // FAQ data
    $faqs = [
        [
            'question' => 'ฉันสามารถขอสินเชื่อซื้อมอเตอร์ไซค์ได้ไหม?',
            'answer' => 'คำตอบ: ใช่ค่ะ คุณสามารถขอสินเชื่อซื้อมอเตอร์ไซค์ได้ แต่คุณจะต้องมีสิทธิ์ในการยืมเงิน และมีประวัติการชำระหนี้ที่ดี เช่น มีงานทำและมีเงินเดือนประมาณ 10,000 บาทขึ้นไป'
        ],
        [
            'question' => 'ฉันต้องการจ่ายค่างวดรายเดือนเท่าไหร่เมื่อฉันขอสินเชื่อซื้อมอเตอร์ไซค์?',
            'answer' => 'คำตอบ: การชำระเงินของค่างวดรายเดือนจะขึ้นอยู่กับยอดเงินที่คุณขอยืม ระยะเวลาการกู้ยืม และอัตราดอกเบี้ย โดยปกติแล้ว ค่างวดรายเดือนจะต้องชำระภายใน 12-60 เดือน'
        ],
        [
            'question' => 'ฉันต้องทำอย่างไรเมื่อฉันไม่สามารถชำระค่างวดของสินเชื่อได้?',
            'answer' => 'คำตอบ: หากคุณไม่สามารถชำระค่างวดของสินเชื่อได้ คุณควรติดต่อธนาคารหรือสถาบันการเงินที่คุณยืมเงินเพื่อขอขยายระยะเวลาการชำระหนี้ ซึ่งอาจมีค่าใช้จ่ายเพิ่มเติม หากคุณไม่สามารถชำระหนี้ได้ตามกำหนด ธนาคารสามารถจะดำเนินการทางกฎหมายเพื่อเรียกเก็บเงินจากคุณได้'
        ],
    ];

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">คำถามที่พบบ่อย คำนวณค่างวดมอเตอร์ไซค์</h2>

    <div class="car-payment-faq-container">
        <?php foreach ($faqs as $faq): ?>
            <div class="car-payment-faq-item">
                <div class="car-payment-faq-question">
                    <?php echo esc_html($faq['question']); ?>
                    <span class="arrow-container">
                        <i class="fas fa-chevron-down"></i>
                    </span>
                </div>
                <div class="car-payment-faq-answer"><?php echo esc_html($faq['answer']); ?></div>
            </div>
        <?php endforeach; ?>
    </div>

    <style>
        /* FAQ Container */
        .car-payment-faq-container {
            margin-top: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
            overflow: hidden;
        }

        /* FAQ Items */
        .car-payment-faq-item {
            padding: 15px 20px;
            cursor: pointer;
            border-bottom: 1px solid #ddd;
            position: relative;
            transition: background-color 0.3s ease;
        }

        .car-payment-faq-item:last-child {
            border-bottom: none;
        }

        /* Hover Effect */
        .car-payment-faq-item:hover {
            background-color: #f9f9f9;
        }

        /* FAQ Question */
        .car-payment-faq-question {
            font-weight: bold;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 16px;
            color: #262626;
            line-height: 22px;
            font-weight: bold;
        }

        /* Arrow Styling */
        .arrow-container {
            transition: transform 0.3s ease;
        }

        .car-payment-faq-item.active .arrow-container {
            transform: rotate(180deg);
        }

        /* FAQ Answer */
        .car-payment-faq-answer {
            display: none;
            margin-top: 10px;
            color: #555;
            line-height: 1.6;
            transition: all 0.3s ease;
            font-family: "Roboto";
            font-size: 14px;
        }
    </style>

    <script>
        document.addEventListener('DOMContentLoaded', () => {
            const faqItems = document.querySelectorAll('.car-payment-faq-item');

            faqItems.forEach(item => {
                item.addEventListener('click', () => {
                    const answer = item.querySelector('.car-payment-faq-answer');

                    // Toggle visibility
                    if (answer.style.display === 'block') {
                        answer.style.display = 'none';
                        item.classList.remove('active');
                    } else {
                        // Hide other open answers
                        faqItems.forEach(i => {
                            i.querySelector('.car-payment-faq-answer').style.display = 'none';
                            i.classList.remove('active');
                        });

                        // Show the current answer
                        answer.style.display = 'block';
                        item.classList.add('active');
                    }
                });
            });
        });
    </script>



<?php
    return ob_get_clean(); // Return the buffered output
}
add_shortcode('motor_payment_faqs', 'motor_payment_faqs');
?>
<?php
function motor_loan_intro_shortcode()
{
    ob_start();
?>
    <div class="intro-section">
        <h2 class="wa-title-text">สินเชื่อรถมอเตอร์ไซค์: วงเงิน, ดอกเบี้ย, ค่างวด ที่ต้องการรู้</h2>
        <div class="content-box">
            <p>เมื่อเราต้องการซื้อมอเตอร์ไซค์ แต่เงินไม่เพียงพอเราจะต้องมองหาวิธีการในการขอสินเชื่อซื้อมอเตอร์ไซค์ เพื่อช่วยให้เราสามารถซื้อมอเตอร์ไซค์ได้ทันทีโดยไม่ต้องรอสะสมเงินมากมายก่อน แต่เมื่อเราต้องการขอสินเชื่อซื้อมอเตอร์ไซค์ จะต้องทราบวิธีการขอสินเชื่อซื้อมอเตอร์ไซค์ และการจัดการเรื่องเอกสารให้เรียบร้อยเพื่อป้องกันการถูกปฏิเสธ เพื่อทำให้การขอสินเชื่อซื้อมอเตอร์ไซค์ของคุณเป็นไปได้ด้วยสะดวกและรวดเร็ว
                <span id="dots">...</span>
            </p>
            <div id="more-text" class="hidden-text">
                <p>วิธีการขอสินเชื่อซื้อมอเตอร์ไซค์ในประเทศไทยไม่ต่างจากการขอสินเชื่อส่วนใหญ่ โดยสิ่งที่เราต้องทำคือติดต่อธนาคารหรือสถาบันการเงินที่เราต้องการขอสินเชื่อซื้อมอเตอร์ไซค์ แล้วเตรียมเอกสารสำหรับการขอสินเชื่อไว้ให้เรียบร้อย โดยเอกสารที่ต้องเตรียมมีดังนี้</p>
                <ul>
                    <li>สำเนาบัตรประชาชน</li>
                    <li>สำเนาทะเบียนบ้าน</li>
                    <li>สลิปเงินเดือน หรือเอกสารประกอบการยืนยันรายได้</li>
                </ul>
                <p>โดยเมื่อได้รับการอนุมัติสินเชื่อซื้อมอเตอร์ไซค์ ธนาคารจะดำเนินการโอนเงินไปยังผู้ขายของมอเตอร์ไซค์ มอเตอร์ไซค์ที่คุณต้องการภายในเวลาอันสั้น ๆ หลังจากนั้น คุณจะต้องชำระเงินผ่อนคงเหลือตามเงื่อนไขที่เราตกลงกันไว้กับธนาคาร เช่น ชำระเงินผ่อนทุกๆ วันที่ 1 ของเดือน หรือชำระเงินผ่อนทุกๆ 15 วัน ขึ้นอยู่กับเงื่อนไขของสัญญาที่คุณได้ทำกับธนาคาร
                </p>
                <p>นอกจากนี้ ยังมีหลายสิ่งที่คุณควรรู้และควรทำเมื่อขอสินเชื่อซื้อมอเตอร์ไซค์ เช่น การตรวจสอบดอกเบี้ยสินเชื่อซื้อมอเตอร์ไซค์ การศึกษาเงื่อนไขของสัญญาให้ละเอียด ๆ การคิดค่าใช้จ่ายและความสามารถในการชำระหนี้และอื่น ๆ</p>
                <p>นอกจากนี้ คุณยังควรเลือกธนาคารหรือสถาบันการเงินที่มีชื่อเสียงและเชื่อถือได้ เพื่อป้องกันการโดนเจ้าหน้าที่ตรวจสอบเอกสารซ้ำซ้อนหรือการตรวจสอบสภาพเครื่องยนต์ของมอเตอร์ไซค์ที่ไม่สมบูรณ์</p>
                <p>ในสรุป การขอสินเชื่อซื้อมอเตอร์ไซค์ในประเทศไทยมีขั้นตอนที่เรียบง่าย โดยให้คุณเตรียมเอกสารที่เกี่ยวข้องกับการขอสินเชื่อไว้ให้เรียบร้อย โดยเลือกธนาคารหรือสถาบันการเงินที่มีชื่อเสียงและเชื่อถือได้ และตรวจสอบดอกเบี้ยสิน</p>
            </div>
            <button id="read-more-btn" class="read-more-btn">อ่านเพิ่มเติม</button>
        </div>
    </div>
    <script>
        document.getElementById("read-more-btn").addEventListener("click", function() {
            var moreText = document.getElementById("more-text");
            var dots = document.getElementById("dots");
            var btnText = document.getElementById("read-more-btn");

            // Toggle visibility of the moreText
            if (moreText.classList.contains("hidden-text")) {
                moreText.classList.remove("hidden-text");
                dots.style.display = "none";
                btnText.innerHTML = "ซ่อน";
            } else {
                moreText.classList.add("hidden-text");
                dots.style.display = "inline";
                btnText.innerHTML = "อ่านเพิ่มเติม";
            }
        });
    </script>
    <style>
        .intro-section {
            /* max-width: 74%; */
        }

        .content-box {
            background-color: #F9F9F9;
            padding: 15px;

        }

        .content-box h4 {
            font-family: 'Roboto' !important;
            font-size: 16px !important;
            color: #262626;
        }

        .hidden-text {
            display: none;
        }

        .read-more-btn {
            background: none;
            border: none;
            color: #576b95;
            cursor: pointer;
            font-size: 14px;
            padding: 0;
        }

        .read-more-btn:hover {
            text-decoration: underline;
        }

        #dots {
            display: inline;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('motor_loan_intro', 'motor_loan_intro_shortcode');
