<?php
function newbike_overview($atts)
{
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

    ob_start();
?>
    <div class="new-cars-overview">
        <span class="overview-text">
          Bạn muốn biết thêm về chiếc mô tô mơ ước của mình, nóng lòng muốnicầm lái chúng? AutoFun có thể giúp bạn! Tại đây bạn có thể xem mọi thông tin của các thương hiệu xe máy nổi tiếng. Tra cứu bảng giá bán, thông số kỹ thuật, màu sắc, hình ảnh và video xe máy mới nhất. Nếu bạn phân vân chưa biết mua chiếc xe máy nào, hãy sử dụng công cụ so sánh xe máy của chúng tôi để biết thêm nhiều thông'
            <span class="hidden-content">
              tin hữu ích như đánh giá của chuyên gia, đánh giá của người từng sử dụng để đưa ra sự chọn lựa thích hợp cho chính mình. . Ngoài ra bạn có thể chọn đại lý xe hai bánh gần nhất trên AutoFun và nhận báo giá.
            </span>
        </span>
        <a class="toggle-content-btn">Đọc thêm</a>
    </div>
    <style>
        .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:30px;
        }

        .hidden-content {
            display: none;
        }

        .toggle-content-btn {
            margin-top: 10px;
            color: #576b95;
            border: none;
            padding: 5px 10px;
            cursor: pointer;
            border-radius: 3px;
			font-weight:700;
			font-size:16px;
			font-family:'Roboto';
        }

        .toggle-content-btn:hover {
           color: #576b95;
        }
			@media screen and (max-width: 768px) {
			   .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:0px;
        }
		}
    </style>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const toggleButton = document.querySelector('.toggle-content-btn');
            const hiddenContent = document.querySelector('.hidden-content');

            if (toggleButton && hiddenContent) {
                toggleButton.addEventListener('click', function() {
                    if (hiddenContent.style.display === 'none' || hiddenContent.style.display === '') {
                        hiddenContent.style.display = 'inline';
                        toggleButton.textContent = 'Ẩn';
                    } else {
                        hiddenContent.style.display = 'none';
                        toggleButton.textContent = 'Đọc thêm';
                    }
                });
            }
        });
    </script>
<?php
    return ob_get_clean();
}

add_shortcode('newbike_overview', 'newbike_overview');
