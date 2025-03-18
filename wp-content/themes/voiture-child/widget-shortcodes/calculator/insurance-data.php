<?php
include_once get_stylesheet_directory() . '/json-ld/faq-json-ld.php';

function insurance_faqs()
{
    // FAQ data
    $faqs = [
        [
            'question' => 'Bảo hiểm xe ô tô bắt buộc là gì?',
            'answer' => 'Bảo hiểm ô tô bắt buộc hay còn gọi là bảo hiểm bắt buộc trách nhiệm dân sự của chủ xe cơ giới đối với bên thứ ba. Loại bảo hiểm này là loại bảo hiểm bắt buộc đối với chủ xe bởi nó sẽ giúp bảo vệ và giảm thiểu thiệt hại cho bên thứ ba và cho chính chủ xe. Theo Nghị định 46 năm 2016, nếu không mang bảo hiểm TNDS, chủ xe ô tô sẽ bị xử phạt 400.000 – 600.000 VNĐ.'
        ],
        [
            'question' => 'Mua bảo hiểm vật chất xe ô tô ở đâu?',
            'answer' => 'Hiện nay có rất nhiều sản phẩm bảo hiểm ô tô và đơn vị cung cấp bảo hiểm ô tô tại Việt Nam.'
        ],
        [
            'question' => 'Phí bảo hiểm xe ô tô là bao nhiêu?',
            'answer' => 'Mức phí bảo hiểm trách nhiệm dân sự ô tô với từng loại xe sẽ khác nhau, dao động từ 437.000 – 4.850.000 VNĐ.'
        ],
    ];

    add_faq_json_ld($faqs);

    ob_start(); // Start output buffering
?>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" integrity="sha384-k6RqeWeci5ZR/Lv4MR0sA0FfDOM7h6p3+NFldf3NOK5GT6Z3F68b9cJ2+Qp8V3b" crossorigin="anonymous" />

    <h2 class="wa-title-text">Câu Hỏi Thường Gặp về Bảo Hiểm Xe</h2>

    <div class="insurance-faq-container">
        <?php foreach ($faqs as $faq): ?>
            <div class="insurance-faq-item">
                <div class="insurance-faq-question">
                    <?php echo esc_html($faq['question']); ?>
                    <!-- <span class="arrow"><i class="fas fa-chevron-down"></i></span> -->
                </div>
                <div class="insurance-arrow-container">
                    <span class="arrow"><i class="fas fa-chevron-down"></i></span> <!-- Font Awesome down arrow -->
                </div>
                <div class="insurance-faq-answer"><?php echo esc_html($faq['answer']); ?></div>
                <hr /> <!-- Line separator between FAQs -->
            </div>
        <?php endforeach; ?>
    </div>

    <style>
        .insurance-faq-container {
            /* max-width: 74%; */
            margin-top: 20px;
            border: 1px solid #ddd;
            /* Outer border for the entire container */
            border-radius: 5px;
            overflow: hidden;
            /* Ensure border radius works */
        }

        .insurance-faq-item {

            cursor: pointer;
            position: relative;
            /* Make it a positioned element */
        }

        .insurance-faq-item:last-child hr {
            display: none;
            /* Hide the last line separator */
        }

        .insurance-faq-question {
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

        .insurance-faq-answer {
            overflow: hidden;
            padding: 0 44px 16px 16px;
            font-size: 14px;
            color: #262626;
            line-height: 22px;
            transition: all .2s;
        }

        .insurance-faq-item:hover {
            background-color: #f5f5f5;
            /* Optional hover effect */
        }

        .insurance-arrow-container {
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

        .insurance-faq-item.active .insurance-arrow-container {
            transform: translateY(-50%) rotate(180deg);
            /* Rotate arrow when active */
        }
    </style>

    <script>
        // Ensure all answers are initially hidden
        document.querySelectorAll('.insurance-faq-answer').forEach(answer => {
            answer.style.display = 'none';
        });

        document.querySelectorAll('.insurance-faq-item').forEach(item => {
            item.addEventListener('click', () => {
                const answer = item.querySelector('.insurance-faq-answer');
                const isActive = item.classList.contains('active');

                // Close all other FAQs
                document.querySelectorAll('.insurance-faq-item.active').forEach(activeItem => {
                    if (activeItem !== item) {
                        activeItem.classList.remove('active');
                        activeItem.querySelector('.insurance-faq-answer').style.display = 'none';
                    }
                });

                // Toggle current FAQ
                if (isActive) {
                    // If the current FAQ is active, close it
                    answer.style.display = 'none';
                    item.classList.remove('active');
                } else {
                    // Otherwise, open it
                    answer.style.display = 'block';
                    item.classList.add('active');
                }
            });
        });
    </script>


<?php
    return ob_get_clean(); // Return the buffered output
}
add_shortcode('insurance_faqs', 'insurance_faqs');
?>

<?php
function insurance_intro_shortcode()
{
//     $no_claim_discount = [
//         ['coverage_duration' => '1st year', 'discount' => '25%'],
//         ['coverage_duration' => '2nd year', 'discount' => '30%'],
//         ['coverage_duration' => '3rd year', 'discount' => '38.33%'],
//         ['coverage_duration' => '4th year', 'discount' => '45%'],
//         ['coverage_duration' => '5th year', 'discount' => '55%'],
//     ];
    ob_start();
?>
    <div class="intro-section">

        <h2 class="wa-title-text">Bảo Hiểm Xe - Máy Tính Bảo Hiểm Việt Nam | AutoFun</h2>
        <div class="insurance-content-box">
<!--             <h4>Car Insurance Introduction</h4> -->
            <p>Chủ xe phải mua các loại bảo hiểm ô tô bắt buộc để có thể sử dụng xe tại Việt Nam. Ngoài ra còn có các loại bảo hiểm tự nguyện giúp giảm thiểu thiệt hại nếu xe gặp sự cố. Dưới đây là những loại bảo hiểm xe ô tô mà chủ xe nên quan tâm.
                <span id="insurance-dots">...</span>
            </p>
            <div id="insurance-more-text" class="insurance-hidden-text">
<!--                 <p>compulsory in Malaysia with the minimum requirement being the third-party insurance. Without a car insurance policy, the road tax could not be renewed for your car.</p> -->
                <h4>Các loại bảo hiểm xe ô tô bắt buộc và tự nguyện hiện nay</h4>
                <h4>Bảo hiểm Trách nhiệm dân sự (TNDS) bắt buộc cho xe ô tô</h4>
				<p>Chủ xe cơ giới cần phải mua Bảo hiểm Trách nhiệm dân sự theo Nghị định số 03/2021/NĐ-CP. Bảo hiểm TNDS là bảo hiểm ô tô bắt buộc áp dụng cho mọi loại xe tại Việt Nam. Loại bảo hiểm này nhằm bảo vệ quyền lợi cho nạn nhân vì những thiệt hại gây ra bởi chủ xe.</p><br>
<p>Phạm vi bồi thường thiệt hại:</p><br>
				<p>Thiệt hại về sức khỏe và tính mạng của hành khách do xe gây ra</p><br>
				<p>Thiệt hại về sức khỏe và tính mạng, tài sản của bên thứ 3 - nạn nhân do xe gây ra.</p><br>
<p>Bảo hiểm sẽ chi trả 100 triệu đồng/người/vụ tai nạn. Nếu thiệt hại tài sản thì sẽ được bồi thường 50 triệu đồng/vụ.</p><br>
               <h4>
				   Bảo hiểm tai nạn cho người lái và hành khách trên xe
				</h4>
				<p>
					Loại này là bảo hiểm ô tô tự nguyện. Bảo hiểm sẽ chịu thiệt hại về tính mạng, thân thể do tai nạn giao thông. Phạm vi chi trả tùy vào chính sách công ty, quyền lợi của gói bảo hiểm mà khách đã mua.
				</p>
				<h4>
					Bảo hiểm vật chất xe ô tô
				</h4>
				<p>
					Bảo hiểm vật chất xe ô tô là một loại bảo hiểm ô tô tự nguyện. Bảo hiểm chịu trách nhiệm bảo vệ thân vỏ, thiết bị của xe ô tô.  Nếu có tai nạn xảy ra, dịch vụ bảo hiểm sẽ hỗ trợ chi phí để khắc phục thiệt hại. Các thiệt hại bao gồm trầy xước, móp méo, cháy nổ,… sẽ được hỗ trợ một phần hoặc toàn phần. Giá trị bảo hiểm đa dạng nên mức chi trả cũng tùy vào gói mà khách chọn mua.
				</p><br>
				<p>
					Bảo hiểm vật chất xe ô tô không chi trả nếu người lái vi phạm luật giao thông. Một số tai nạn do vận chuyển hàng cấm, do bị tác động ngoại lực không được chi trả. Bảo hiểm cũng không chi trả tổn thất nếu xe bị mất cắp ngoài lãnh thổ Việt Nam.
				</p>
				<h4>
					Bảo hiểm vật chất mở rộng
				</h4>
				<p>
					Gói này nhằm tăng thêm phạm vi bảo hiểm vật chất xe ô tô. Nó sẽ chi trả cho các trường hợp ngoài gói bảo hiểm nói trên. Ví dụ như: mất cắp bộ phận, xe bị ngập nước…
				</p>
				<h4>
					Bảo hiểm TNDS tự nguyện
				</h4>
				<p>
					Đây là bảo hiểm thể hiện trách nhiệm tăng thêm ngoài mức bảo hiểm bắt buộc của nhà nước. Bảo hiểm sẽ trả lại phần tiền chênh lệch mà chủ xe bồi thường cho người bị hại. Mức chi trả dựa trên số tiền mà chủ xe đã đóng bảo hiểm.
				</p>
				<h4>
					Bảo hiểm TNDS hàng hóa
				</h4>
				<p>
					Bảo hiểm này chi trả thiệt hại về hàng hóa do tai nạn gây ra trong quá trình vận chuyển. Mức bồi thường sẽ được căn cứ theo quy định của Luật dân sự.
				</p><br>
				<p>
					Chủ xe nên mua cùng lúc bảo hiểm 2 chiều: Bảo hiểm TNDS và Bảo hiểm vật chất ô tô. Khi xảy ra tai nạn, bảo hiểm TNDS sẽ chi trả cho người bị hại. Còn bảo hiểm vật chất sẽ bồi thường tổn thất của xe.
				</p>
				<h4>
					Mức phạt khi không có bảo hiểm xe ô tô bắt buộc
				</h4>
				<p>
					Xe lưu thông trên lãnh thổ Việt Nam mà không có bảo hiểm TNDS sẽ bị vi phạm hành chính. Phạt tiền từ 400.000 – 600.000 đồng cho mỗi lần vi phạm.
				</p>
				<h4>
					Bảo hiểm ô tô bắt buộc không chi trả cho những trường hợp nào?
				</h4>
				<p>
					Chủ xe cố ý gây thiệt hại cho xe mình sở hữu.
				</p><br>
				<p>
					Lái xe cố ý bỏ chạy khi lỡ gây ra tai nạn.
				</p><br><p>
					Tài xế không có giấy phép lái xe gây tai nạn giao thông.
				</p><br><p>
					Tài sản bị mất cắp, bị cướp khi xảy ra tai nạn sẽ không được bảo hiểm chi trả.
				</p><br><p>
					Tài sản có giá trị cao như vàng, bạc, đá quý, tiền, đồ cổ… không được đền bù.
				</p><br>
				<h4>
					Kinh nghiệm mua bảo hiểm ô tô chủ xe cần lưu ý
				</h4>
				<p>
					Các bảo hiểm bắt buộc thì nhất định phải mua để tránh bị phạt hành chính.
				</p><br>
				<p>
					Các loại bảo hiểm tự nguyện thì chủ xe có thể cân nhắc. Tùy vào khả năng tài chính mà chủ xe có thể mua nhiều hoặc ít loại.
				</p><br>
				<p>
					Kinh nghiệm cho thấy chủ xe tốt nhất nên mua 3 loại Bảo hiểm. Đầu tiên là Bảo hiểm Trách nhiệm dân sự bắt buộc. Loại thứ 2 là Bảo hiểm vật chất xe ô tô (có cả gói mở rộng). Loại thứ 3 là Bảo hiểm tai nạn người lái và hành khách trên xe.
				</p><br>
				<p>
					Xe ô tô mới mua, trong 3 năm đầu nên mua bảo hiểm ô tô 2 chiều.
				</p><br>
				<p>
					Xe ô tô cũ thì phải mua Bảo hiểm TNDS là đương nhiên. Ngoài ra cũng có thể cân nhắc mua thêm Bảo hiểm vật chất xe ô tô.
				</p><br>
				<p>
					Đối với loại xe giá trị cao trên 700 triệu, nên mua thêm bảo hiểm mất cắp bộ phận.
				</p><br>
				<p>
					Các loại xe ô tô chạy dịch vụ… vận hành liên tục sẽ dễ gặp tai nạn và mất cắp hơn. Do đó không thể xem thường chuyện mua bảo hiểm.
				</p><br>
				<p>
					Xe ô tô cá nhân thì có thể cân nhắc nên hay không nên mua bảo hiểm tự nguyện.
				</p><br>
				<p>
					Xe hay di chuyển ở nơi đô thị đông đúc thì nên mua bảo hiểm vật chất ô tô. Mật độ xe lưu thông cao không thể tránh khỏi va chạm ngoài ý muốn.
				</p><br>
				<p>
					Xe thường xuyên di chuyển trên các tuyến đường lớn, cao tốc… cũng nên mua bảo hiểm vật chất.
				</p><br>
				<p>
					Nếu ở môi trường dễ bị ngập lụt thì nên mua Bảo hiểm thủy kích ô tô.
				</p><br>
				<h4>
					Thủ tục mua bảo hiểm xe ô tô
				</h4>
				<p>
					Chủ xe có thể mua bảo hiểm ô tô của công ty liên kết với đại lý bán xe. Thủ tục đỡ rắc rối và quá trình mua diễn ra suôn sẻ, bớt mất thời gian. Hoặc cũng có thể mua của công ty liên kết với Xưởng bảo dưỡng xe mà mình tin tưởng.
				</p><br>
				<p>
					Hạn chế mua các loại bảo hiểm ô tô có hình thức ứng tiền trước khi khắc phục tổn thất.
				</p><br>
				<p>
					Nên đọc kỹ hợp đồng để nắm rõ các quy định của bảo hiểm trước khi mua. Đặc biệt nên chú ý đến các trường hợp bảo hiểm không bồi thường để tránh bị thiệt thòi.
				</p>
				<h4>
					Các hãng bảo hiểm ô tô uy tín nhất hiện nay
				</h4>
				<p>
					Bảo hiểm ô tô Bảo Việt: Công ty thành lập từ năm 1965, lâu đời nhất tại Việt Nam. Hãng bán tất cả các loại bảo hiểm xe ô tô, gồm: 
				</p><br>
				<p>
					Bảo hiểm bắt buộc: Bảo hiểm Trách nhiệm dân sự theo Luật hiện hành. </p><br>
				<p>Bảo hiểm phổ thông: Bảo hiểm vật chất xe cơ bản, Bảo hiểm tai nạn hành khách trên xe. </p><br>
<p>Bảo hiểm nâng cao: Gói bảo hiểm vật chất mở rộng, Bảo hiểm mất cắp bộ phận, Bảo hiểm thủy kích…
				</p><br>
				<p>
				Bảo hiểm ô tô PVI: Một trong những doanh nghiệp bảo hiểm số 1 Việt Nam, thành lập từ 2011. Ngoài các gói bảo hiểm trên, PVI còn bán gói Bảo hiểm hàng hóa vận chuyển trên xe ô tô.</p><br>

<p>
	Bảo hiểm ô tô Liberty: Bảo hiểm Mỹ có mặt tại Việt Nam từ 2003. Hãng bán đầy đủ các gói bảo hiểm cần thiết, ngoài ra còn trợ giúp giao thông 24/7.
				</p><br>

<p>
	Bảo hiểm ô tô PJICO: Hãng thành lập năm 1995, là bảo hiểm phi nhân thọ hàng đầu Việt Nam.
				</p><br>

<p>Bảo hiểm ô tô Bưu điện PTI: Hãng kết nối với hệ thống 485 garage sửa xe chính hãng trên toàn quốc. Ngoài ra còn hỗ trợ cẩu kéo xe miễn phí trong bán kính 100 km.</p><br>

<p>Bảo hiểm ô tô Quân đội MIC: Hãng thành lập năm 2007, cung cấp các gói bảo hiểm tốt nhất cho đơn vị thuộc Bộ Quốc Phòng.</p><br>
				<h4>
					Kết luận
				</h4>
				<p>
					Bảo hiểm ô tô là hình thức quản lý rủi ro thông minh mà chủ xe nên đầu tư. Bởi một khi xảy ra tai nạn mà không được hỗ trợ chi trả, thiệt hại sẽ lớn vô cùng.
				</p><br>
				<p>
					Xem thêm:<a href="https://www.autofun.vn/tin-tuc/skills-that-new-drivers-can-not-ne-achieved-54886"> 5 kỹ năng mà tài xế mới lái khó có thể có được</a>
				</p>
            </div>
            <button id="insurance-read-more-btn" class="insurance-read-more-btn">Đọc thêm</button>
        </div>
    </div>
    <script>
        document.getElementById("insurance-read-more-btn").addEventListener("click", function() {
            var moreText = document.getElementById("insurance-more-text");
            var dots = document.getElementById("insurance-dots");
            var btnText = document.getElementById("insurance-read-more-btn");

            // Toggle visibility of the moreText
            if (moreText.classList.contains("insurance-hidden-text")) {
                moreText.classList.remove("insurance-hidden-text");
                dots.style.display = "none";
                btnText.innerHTML = "Ẩn";
            } else {
                moreText.classList.add("insurance-hidden-text");
                dots.style.display = "inline";
                btnText.innerHTML = "Đọc thêm";
            }
        });
    </script>
    <style>
        .dis-container {
            max-height: 273px;
            border: 1px solid #ddd;
            /* max-width: 100%; */
            display: block;
        }

        .dis-row {
            display: flex;
            padding: 10px;
            border-bottom: 1px solid #ddd;
            background: #fff;
        }

        .dis-header {
            background-color: #f8f8f8;
            font-weight: bold;
        }

        .dis-label,
        .dis-value {
            flex: 1;
            display: flex;
            align-items: center;
        }

        .dis-label {
            text-align: left;
            border-right: 1px solid #ccc;
            padding-right: 15px;
            margin-top: -10px;
            margin-bottom: -10px;
        }

        .dis-value {
            text-align: right;
            padding-left: 15px;
        }

        .dis-row:first-child {
            border-top: 1px solid #ddd;
        }

        .dis-row:last-child {
            border-bottom: 1px solid #ddd;
        }



        .insurance-content-box {
            background-color: #F9F9F9;
            padding: 15px;
            font-family: 'Roboto';
            color: #262626;
        }

        .insurance-content-box h4 {
            background-color: #F9F9F9;
            font-family: 'Roboto';
            color: #262626;
        }

        .insurance-hidden-text {
            display: none;
        }

        .insurance-read-more-btn {
            background: none;
            border: none;
            color: #576b95;
            cursor: pointer;
            font-size: 14px;
            padding: 0;
        }

        .insurance-read-more-btn:hover {
            text-decoration: underline;
        }

        #insurance-dots {
            display: inline;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('insurance_intro', 'insurance_intro_shortcode');
