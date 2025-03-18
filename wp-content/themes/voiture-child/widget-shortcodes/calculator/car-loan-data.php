<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function car_loan_data($atts)
{
    $current_year = date('Y');
    $loan_data = [
        ['bank_name' => 'Techcombank', 'interest_rate' => '6.7%/năm'],
        ['bank_name' => 'VPBank', 'interest_rate' => '6.8%/năm'],
        ['bank_name' => 'MBBank', 'interest_rate' => '6.6%/năm'],
        ['bank_name' => 'TPBank', 'interest_rate' => '7.3%/năm'],
        ['bank_name' => 'VIB', 'interest_rate' => '7.4%/năm'],
        ['bank_name' => 'OCB', 'interest_rate' => '8%/năm'],
        ['bank_name' => 'Shinhan', 'interest_rate' => '6%/năm'],
        ['bank_name' => 'SHB', 'interest_rate' => '7.49%/năm'],
        ['bank_name' => 'SCB', 'interest_rate' => '7.9%/năm'],
        ['bank_name' => 'LienVietPostBank', 'interest_rate' => '8%/năm'],
        ['bank_name' => 'HongLeong', 'interest_rate' => '7.29%/năm'],
        ['bank_name' => 'BaoVietBank', 'interest_rate' => '6.99%/năm'],
        ['bank_name' => 'HDBank', 'interest_rate' => '7.9%/năm'],
        ['bank_name' => 'Bac A Bank', 'interest_rate' => '6.39%/năm'],
        ['bank_name' => 'MSB', 'interest_rate' => '6.99%/năm'],
        ['bank_name' => 'PVcomBank', 'interest_rate' => '6.49%/năm'],
		
		
    ];
?>
    <h2 class="wa-title-text">Lãi Suất Mua Xe Trả Góp <?php echo $current_year; ?></h2>
    <div class="loan-data-container">
        <div class="loan-data-row loan-data-header">
            <div class="loan-data-label">Ngân Hàng</div>
            <div class="loan-data-value">Lãi suất</div>
        </div>
        <?php
        // Loop through the array to display the data
        foreach ($loan_data as $loan) {
        ?>
            <div class="loan-data-row">
                <div class="loan-data-label"><?php echo $loan['bank_name']; ?></div>
                <div class="loan-data-value"><?php echo $loan['interest_rate']; ?></div>
            </div>
        <?php
        }
        ?>
    </div>

    <style>
        .loan-data-container {
            max-height: 273px;
            overflow-y: scroll;
            /* Enable vertical scroll */
            overflow-x: hidden;
            /* Prevent horizontal scroll */
            border: 1px solid #ddd;
            /* max-width: 74%; */
            display: block;
        }

        .loan-data-container::-webkit-scrollbar {
            width: 0;
            height: 0;
        }

        .loan-data-container {
            scrollbar-width: none;
            /* Firefox */
        }

        .loan-data-row {
            display: flex;
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }

        .loan-data-header {
            background-color: #f8f8f8;
            font-weight: bold;
        }

        .loan-data-label,
        .loan-data-value {
            flex: 1;
            display: flex;
            align-items: center;
        }

        .loan-data-label {
            text-align: left;
            border-right: 1px solid #ccc;
            padding-right: 15px;
            margin-top: -10px;
            margin-bottom: -10px;
        }

        .loan-data-value {
            text-align: right;
            padding-left: 15px;
        }

        .loan-data-row:first-child {
            border-top: 1px solid #ddd;
        }

        .loan-data-row:last-child {
            border-bottom: 1px solid #ddd;
        }
    </style>

<?php
}
add_shortcode('car_loan_data', 'car_loan_data');
?>

<?php
function car_payment_faqs()
{
    // FAQ data
   $faqs = [
    [
        'question' => 'Mua xe ô tô trả góp phải trả trước bao nhiêu tiền?',
        'answer' => 'Thông thường bạn sẽ phải trả trước tối thiểu 10% đến 30% trên tổng giá trị của chiếc xe.'
    ],
    [
        'question' => 'Mua xe ô tô trả góp có cần chứng minh thu nhập không?',
        'answer' => 'Người vay Bắt Buộc phải chứng minh thu nhập khi muốn mua xe trả góp.'
    ],
    [
        'question' => 'Có nên mua xe ô tô trả góp không?',
        'answer' => 'Nếu bạn có thể mua ngay thì không cần nghĩ đến việc mua xe trả góp. Nếu bạn không đủ tiền và đủ khả năng trả nợ, bạn có thể cân nhắc mua xe trả góp.'
    ],
];


    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">Câu Hỏi Thường Gặp về Mua Xe Trả Góp</h2>

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
            font-size: 14px;
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
add_shortcode('car_payment_faqs', 'car_payment_faqs');
?>
<?php
function car_loan_intro_shortcode()
{
    ob_start();
?>
<div class="intro-section">
        <h2 class="wa-title-text">Mua Xe Trả Góp - Bảng Tính Chi Phí Mua Xe Ô Tô Trả Góp</h2>
        <div class="content-box">
<!--             <h4>Buying a car and applying for a loan: an easy matter that is easy to understand</h4> -->
            <p>Mua xe ô tô trả góp là hình thức dễ dàng nhất để bạn có thể sở hữu một chiếc từ 4 chỗ trở lên. Không áp lực tài chính, thủ tục đơn giản, thực hiện nhanh chóng. Nếu bạn đang có ý định mua xe trả góp thì dưới đây là vài điều cần lưu ý. 
                <span id="dots">...</span>
            </p>
            <div id="more-text" class="hidden-text">
                <p>the leasing company acts as a middleman, paying the car manufacturer before allowing customers to pay in installments along with interest. While wealthy individuals may purchase cars with cash, car financing is essential for most people.
                </p>
                <h4>Mua xe ô tô trả góp là gì?</h4>
                <p>
                   Sau khi chọn được chiếc xe mong muốn, bạn sẽ phải thanh toán trước một phần tiền tự có. Phần còn lại dưới dạng một khoản vay mà bạn làm việc với ngân hàng. Khoản này sẽ trả dần theo thỏa thuận với bên ngân hàng. Thời gian vay kéo dài từ 1 - 5 năm tùy mức mà có thể bạn chi trả từng tháng. Đây chính là hình thức mua xe ô tô trả góp đang áp dụng tại thị trường Việt Nam.
                </p>
            <p>
				
				Trong thời gian trả góp, chiếc xe là vật thế chấp cho ngân hàng. Ngân hàng sẽ giữ bản gốc đăng ký xe ô tô. Một số ngân hàng có thể cho vay đến 8 năm nên bạn hoàn toàn không cần lo lắng.
				</p>
                <h4>Mua xe ô tô trả góp cần quan tâm những vấn đề gì?</h4>
                <p>
                   Trước khi muốn mua xe trả góp, bạn cần cân nhắc khá nhiều vấn đề. Đầu tiên là khả năng tài chính cá nhân, đã có sẵn bao nhiêu tiền. Cân nhắc lựa chọn mẫu xe phù hợp với bản thân.

                </p>
                <p>
                   Thứ hai là cần phải vay thêm bao nhiêu tiền để đủ mua chiếc ô tô mình mong muốn. Lựa chọn tổ chức tín dụng/ngân hàng phù hợp. Tham khảo về mức lãi suất mình cần phải trả. 
                </p>
                <p>
                    Cuối cùng là khả năng chi trả khoản vay cá nhân. Ban cần mất bao nhiêu thời gian để chi trả? Mức trả hàng tháng có vượt quá khả năng chi tiêu hay không?
                </p>
            </div>
            <div class="read-more-btn-loan">
                <button id="read-more-btn" class="read-more-btn">Đọc thêm</button>		
            </div>
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
                btnText.innerHTML = "Ẩn";
            } else {
                moreText.classList.add("hidden-text");
                dots.style.display = "inline";
                btnText.innerHTML = "Đọc thêm";
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
		.content-box p {
		   margin-top: -15px;
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
		    margin-top: -15px;
			font-weight:700;
        }
		.read-more-btn-loan{
			display:flex;
			justify-content:end;
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
add_shortcode('car_loan_intro', 'car_loan_intro_shortcode');
