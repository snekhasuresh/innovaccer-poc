<?php
function newcars_overview($atts)
{
$translate = [
    
'news car' => 'Trang web cung cấp thông tin đầy đủ và mới nhất về các dòng xe và các thương hiệu xe tại Việt Nam. Tra cứu các thông tin về xe như thông số kỹ thuật, tin tức, giá bán, tính năng, hình ảnh, video, đánh giá của chuyên gia và người dùng trước khi bạn mua xe. Người dùng có thể thông qua tra cứu các dữ liệu trên website của chúng tôi như thương hiệu, ngân sách, loại nhiên liệu, chỗ ngồi, phân khúc, loại thân xe, hộp số hoặc bất kỳ thứ gì khác mà bản thân quan tâm, tìm ra được chiếc xe ưng ý cho chính mình.',

];
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

    // Output the HTML directly without concatenating or echoing
?>
    <div class="new-cars-overview">
        <span><?php echo $translate['news car']; ?></span>
    </div>
    <style>
        .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:30px;
        }
		.new-cars-overview span{
			font-size:14px;
			font-family:'Roboto';
			color:#262626;
			font-weight:400
		}
		@media screen and (max-width: 768px) {
			   .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:0px !important;
        }
		}
    </style>

<?php
}

add_shortcode('newcars_overview', 'newcars_overview');
