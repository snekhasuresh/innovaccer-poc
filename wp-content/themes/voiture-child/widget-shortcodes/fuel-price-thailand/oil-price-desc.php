<?php
function oil_price_desc_shortcode()
{
    ob_start();
?>
    <div class="intro-section">
        <h2 class="wa-title-text">ราคาน้ำมันวันนี้ เบนซิน 95 แก๊สโซฮอล์ 95 91 ราคาน้ำมันดีเซล ราคา NGV</h2>
        <div class="content-box">
            <p>Dec 29 2024 อัพเดทราคาน้ำมันวันนี้ประเทศไทย แก๊สโซฮอล์ 95 THB 35.25 บาท/ลิตร, เบนซิน 95 THB 43.14บาท/ลิตร, แก๊สโซฮอล์ 91 THB 33.48 บาท/ลิตร, แก๊สโซฮอล์ E20 THB 33.14 บาท/ลิตร, แก๊สโซฮอล์ E85 THB 33.29 บาท/ลิตร, ดีเซล B7 THB 29.94 บาท/ลิตร, ดีเซล B20 THB 29.94 บาท/ลิตร, แก๊ส NGV THB 17.59 บาท/ลิตร
                <span id="dots">...</span>
            </p>
            <div id="more-text" class="hidden-text">
                <p>ตารางราคาน้ำมันทุกชนิด เบนซิน, แก๊สโซฮอล์, ดีเซล B7,B20 ทุกปั๊ม ปตท PPT, บางจาก Bangchak, เชลล์ Shell, เอสโซ่ Esso, คาลเท็กซ์ Caltex, พีที PT, ซัสโก้ Susco, เพียว PURE และไออาร์พีซี IRPC
                </p>
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
            color: #007bff;
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
add_shortcode('oil_price_desc', 'oil_price_desc_shortcode');
